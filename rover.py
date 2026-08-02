#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SVERK Dozor — узел связи РОВЕРА.

Назначение
----------
Постоянный (heartbeat) обмен сообщениями между ровером и дроном по UDP,
пока оба находятся в одной сети (Wi-Fi/LAN). Это базовый уровень связи —
поверх него позже можно строить обмен целями, детекциями, командами и т.д.
Сообщение — это JSON-словарь, отправляемый по UDP; формат специально
сделан расширяемым (см. build_message).

Как это работает
-----------------
- Каждая сторона раз в HEARTBEAT_INTERVAL секунд рассылает heartbeat
  со своей ролью, стартовой клеткой поля и статусом.
- По умолчанию используется UDP broadcast (255.255.255.255) — тогда
  IP второй стороны знать заранее не нужно, работает "из коробки" в
  одной Wi-Fi сети.
- Если broadcast заблокирован точкой доступа (client isolation) —
  укажите IP дрона явно через --peer-ip или переменную окружения
  DOZOR_PEER_IP, тогда обмен пойдёт напрямую (unicast).
- Сторона считается "на связи", пока heartbeat приходят чаще, чем раз
  в PEER_TIMEOUT секунд; при пропаже связи выводится предупреждение,
  при восстановлении — отдельное сообщение.
- В консоли можно набирать произвольный текст и жать Enter — он уйдёт
  ровером как chat-сообщение (демонстрация "живого" обмена, не только
  heartbeat-ов).

Запуск
------
    python3 dozor_rover_link.py
    python3 dozor_rover_link.py --peer-ip 192.168.1.50
    python3 dozor_rover_link.py --no-chat          # без интерактивного ввода (для systemd/фона)

Дрон запускается аналогичным скриптом dozor_drone_link.py — протокол
и порт у обоих скриптов совпадают, поэтому достаточно, чтобы оба были
в одной сети (или чтобы у одного из них был указан прямой IP другого).
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

# --------------------------------------------------------------------------
# Принудительно работаем в UTF-8 независимо от локали окружения. На части
# устройств/SSH-сессий локаль терминала не UTF-8 (например LANG=C или
# вовсе не задана) — тогда Python читает stdin через errors="surrogateescape"
# и любая введённая кириллица превращается в "битые" суррогатные символы,
# которые роняют программу при попытке отправить их по сети (UnicodeEncodeError
# при .encode("utf-8")). Явно переоткрываем потоки в UTF-8, чтобы кириллица
# всегда читалась и писалась корректно, независимо от локали ОС/терминала.
for _stream in (sys.stdin, sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass  # старый Python без reconfigure() или поток уже недоступен

# --------------------------------------------------------------------------
# Роль этого узла. У дрона (dozor_drone_link.py) эти константы зеркальные.
# --------------------------------------------------------------------------
ROLE = "rover"
PEER_ROLE = "drone"
START_CELL = "D1"  # ровер стартует с клетки D1

# --------------------------------------------------------------------------
# Геометрия поля (см. aruco_map.txt из проекта): 6x6 клеток по 0.8 м,
# столбцы A..F (ось X), строки 1..6 (ось Y), нумерация с нижнего левого угла.
# Пригодится на следующих этапах (навигация), сейчас используется только
# для того, чтобы транслировать партнёру человеко-читаемые координаты.
# --------------------------------------------------------------------------
CELL_SIZE_M = 0.8
GRID_COLS = "ABCDEF"


def cell_to_xy(cell: str) -> tuple[float, float]:
    """Преобразовать обозначение клетки поля (например 'A6') в метры (x, y)."""
    cell = cell.strip().upper()
    col_letter, row_str = cell[0], cell[1:]
    col_idx = GRID_COLS.index(col_letter) + 1  # 1..6
    row_idx = int(row_str)                     # 1..6
    x = (col_idx - 0.5) * CELL_SIZE_M
    y = (row_idx - 0.5) * CELL_SIZE_M
    return round(x, 3), round(y, 3)


# --------------------------------------------------------------------------
# Сетевые настройки (можно переопределить через переменные окружения
# или аргументы командной строки — аргументы командной строки в приоритете)
# --------------------------------------------------------------------------
DEFAULT_PORT = int(os.environ.get("DOZOR_PORT", "45454"))
DEFAULT_BROADCAST_IP = os.environ.get("DOZOR_BROADCAST_IP", "255.255.255.255")
DEFAULT_PEER_IP = os.environ.get("DOZOR_PEER_IP")  # None => используем broadcast
DEFAULT_INTERVAL = float(os.environ.get("DOZOR_HEARTBEAT_INTERVAL", "2.0"))
DEFAULT_TIMEOUT = float(os.environ.get("DOZOR_PEER_TIMEOUT", "6.0"))

# --------------------------------------------------------------------------
# Обходной канал через MCP (порт 9092), если прямой UDP-порт 45454 не
# пробрасывается наружу контейнера партнёра (частая ситуация, когда сам
# приёмник крутится в изолированной docker-сети без публикации UDP-порта,
# а HTTP-порт MCP уже опубликован). Работает так: вместо отправки UDP-пакета
# по сети напрямую, мы просим shell_exec партнёра отправить этот же пакет
# себе на 127.0.0.1 — то есть локально, внутри его собственного netns, где
# никакого файрвола/NAT между отправителем и recv_sock уже нет.
# Включается флагом --mcp-relay (или переменной DOZOR_MCP_URL).
# --------------------------------------------------------------------------
DEFAULT_MCP_URL = os.environ.get("DOZOR_MCP_URL")  # напр. http://192.168.1.36:9092
DEFAULT_MCP_TIMEOUT = float(os.environ.get("DOZOR_MCP_TIMEOUT", "5.0"))

INSTANCE_ID = uuid.uuid4().hex[:8]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(ROLE)


def build_message(msg_type: str, seq: int, extra: dict | None = None) -> dict:
    """Собрать сообщение протокола. Формат намеренно расширяемый (extra)."""
    x, y = cell_to_xy(START_CELL)
    msg = {
        "type": msg_type,          # "hello" | "heartbeat" | "chat" | "bye"
        "from": ROLE,
        "to": PEER_ROLE,
        "seq": seq,
        "ts": time.time(),
        "instance": INSTANCE_ID,
        "position": {"cell": START_CELL, "x": x, "y": y},
        "status": "online",
    }
    if extra:
        msg.update(extra)
    return msg


class DozorLink:
    """UDP-канал постоянного обмена сообщениями с партнёром (ровер/дрон)."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.stop_event = threading.Event()
        self.seq = 0
        self.seq_lock = threading.Lock()

        self.peer_last_seen: float | None = None
        self.peer_last_seq: int | None = None
        self.peer_ip: str | None = None
        self.state_lock = threading.Lock()

        # Отдельный сокет на отправку (с флагом broadcast) и на приём.
        self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Примечание: SO_REUSEPORT намеренно НЕ используется — при двух
        # процессах на одном порту он заставляет ядро ОС отдавать каждый
        # входящий пакет только одному из слушателей (псевдослучайно), что
        # ломает обмен при тестировании дрона и ровера на одной машине.
        # В реальных условиях дрон и ровер — разные физические хосты, так
        # что коллизии портов не возникает в принципе.
        self.recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.recv_sock.bind(("0.0.0.0", args.local_port))
        self.recv_sock.settimeout(1.0)

        target_ip = args.peer_ip or args.broadcast_ip
        self.peer_addr = (target_ip, args.port)
        self._threads: list[threading.Thread] = []
        self._mcp_warned = False

    # -- отправка -----------------------------------------------------
    def send(self, msg_type: str, extra: dict | None = None) -> dict:
        with self.seq_lock:
            self.seq += 1
            seq = self.seq
        msg = build_message(msg_type, seq, extra)
        raw = json.dumps(msg, ensure_ascii=False)
        try:
            data = raw.encode("utf-8")
        except UnicodeEncodeError as exc:
            # Подстраховка на случай "битых" суррогатных символов в тексте
            # (обычно из-за не-UTF-8 локали терминала при вводе с клавиатуры).
            # Не роняем узел связи из-за одного плохого сообщения — заменяем
            # проблемные символы и отправляем дальше.
            log.warning(
                "Не удалось закодировать сообщение (%s) в UTF-8 (%s), "
                "отправляю с заменой повреждённых символов", msg_type, exc,
            )
            data = raw.encode("utf-8", errors="replace")
        try:
            self.send_sock.sendto(data, self.peer_addr)
        except OSError as exc:
            log.warning("Не удалось отправить сообщение (%s): %s", msg_type, exc)
        if self.args.mcp_url:
            self._mcp_relay_send(data)
        return msg

    def _mcp_relay_send(self, data: bytes) -> None:
        """Продублировать пакет через MCP shell_exec партнёра (см. комментарий
        у DEFAULT_MCP_URL выше). Партнёр локально отправит этот же пакет
        себе на 127.0.0.1:<port>, где его подхватит обычный recv_sock —
        так heartbeat доходит, даже если прямой UDP до партнёра снаружи
        заблокирован файрволом/докер-сетью."""
        payload_b64 = base64.b64encode(data).decode("ascii")
        # port внутри netns партнёра — тот же UDP-порт, на котором слушает его recv_sock
        py_cmd = (
            "import socket,base64;"
            f"d=base64.b64decode('{payload_b64}');"
            "s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);"
            f"s.sendto(d,('127.0.0.1',{self.args.port}));"
            "s.close()"
        )
        body = json.dumps({
            "name": "shell_exec",
            "arguments": {"command": f"python3 -c \"{py_cmd}\"", "timeout": 5},
        }).encode("utf-8")
        req = urllib.request.Request(
            self.args.mcp_url.rstrip("/") + "/call",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.args.mcp_timeout) as resp:
                resp.read()  # не разбираем ответ, важен только факт доставки
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if not self._mcp_warned:
                log.warning(
                    "MCP-релей (%s) недоступен: %s — heartbeat идёт только "
                    "прямым UDP, дальнейшие ошибки релея не логируются",
                    self.args.mcp_url, exc,
                )
                self._mcp_warned = True

    # -- приём ----------------------------------------------------------
    def _recv_loop(self):
        while not self.stop_event.is_set():
            try:
                data, addr = self.recv_sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                msg = json.loads(data.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            if msg.get("from") != PEER_ROLE:
                # свои же широковещательные пакеты или посторонний трафик на этом порту
                continue
            self._handle_message(msg, addr)

    def _handle_message(self, msg: dict, addr: tuple[str, int]):
        now = time.time()
        with self.state_lock:
            was_online = self._peer_online_locked(now)
            self.peer_last_seen = now
            self.peer_last_seq = msg.get("seq")
            self.peer_ip = addr[0]

        if not was_online:
            pos = msg.get("position", {})
            log.info(
                "\u2705 %s на связи! IP=%s, стартовая точка=%s",
                PEER_ROLE, addr[0], pos.get("cell", "?"),
            )

        mtype = msg.get("type")
        if mtype == "chat":
            text = msg.get("text", "")
            print(f"\n\U0001F4AC [{PEER_ROLE}] {text}\n> ", end="", flush=True)
        elif mtype == "bye":
            log.info("\U0001F44B %s сообщил(а) о завершении работы", PEER_ROLE)

    # -- состояние связи -------------------------------------------------
    def _peer_online_locked(self, now: float | None = None) -> bool:
        if self.peer_last_seen is None:
            return False
        now = now if now is not None else time.time()
        return (now - self.peer_last_seen) <= self.args.timeout

    def peer_online(self) -> bool:
        with self.state_lock:
            return self._peer_online_locked()

    # -- периодические задачи --------------------------------------------
    def _heartbeat_loop(self):
        self.send("hello")  # сразу объявляем о своём появлении в сети
        while not self.stop_event.is_set():
            self.send("heartbeat")
            self.stop_event.wait(self.args.interval)

    def _status_loop(self):
        prev_online = None
        last_print = 0.0
        while not self.stop_event.is_set():
            online = self.peer_online()
            if prev_online is True and online is False:
                log.warning(
                    "\u26A0\uFE0F  %s пропал со связи (нет heartbeat дольше %.0f с)",
                    PEER_ROLE, self.args.timeout,
                )
            prev_online = online

            now = time.time()
            if now - last_print >= 10.0:
                last_print = now
                with self.state_lock:
                    last_seen = self.peer_last_seen
                    last_seq = self.peer_last_seq
                since = f"{now - last_seen:.1f} с назад" if last_seen else "нет данных"
                state = "ONLINE \u2705" if online else "OFFLINE \u274C"
                log.info(
                    "Статус связи с %s: %s | последний heartbeat: %s | seq=%s",
                    PEER_ROLE, state, since, last_seq,
                )
            self.stop_event.wait(1.0)

    def _interactive_loop(self):
        print(
            "Наберите текст и нажмите Enter, чтобы отправить сообщение "
            f"{PEER_ROLE}-у. 'exit' или Ctrl+C — выход.\n> ",
            end="", flush=True,
        )
        while not self.stop_event.is_set():
            try:
                line = input()
            except EOFError:
                # stdin недоступен (например, запуск в фоне/через systemd) —
                # просто продолжаем работать без интерактивного чата.
                log.info(
                    "stdin недоступен — интерактивный чат отключён, "
                    "работаю в фоне (heartbeat продолжается, Ctrl+C — выход)"
                )
                while not self.stop_event.is_set():
                    time.sleep(1.0)
                return
            except KeyboardInterrupt:
                raise

            text = line.strip()
            if text.lower() in ("exit", "quit", "выход"):
                return
            if text:
                try:
                    self.send("chat", {"text": text})
                except Exception as exc:  # noqa: BLE001 - не роняем чат из-за краевых случаев
                    log.warning("Не удалось отправить сообщение чата: %s", exc)
            print("> ", end="", flush=True)

    # -- жизненный цикл ---------------------------------------------------
    def start(self):
        self._threads = [
            threading.Thread(target=self._recv_loop, name="recv", daemon=True),
            threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True),
            threading.Thread(target=self._status_loop, name="status", daemon=True),
        ]
        for t in self._threads:
            t.start()

    def stop(self):
        self.send("bye", {"status": "offline"})
        time.sleep(0.2)  # дать пакету уйти в сеть перед закрытием сокета
        self.stop_event.set()
        for sock in (self.recv_sock, self.send_sock):
            try:
                sock.close()
            except OSError:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Постоянный обмен сообщениями {ROLE} <-> {PEER_ROLE} по UDP",
    )
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                         help=f"UDP-порт партнёра, на который отправляются сообщения "
                              f"(по умолчанию {DEFAULT_PORT})")
    parser.add_argument("--local-port", type=int, default=None,
                         help="локальный порт для приёма (по умолчанию совпадает с --port; "
                              "отдельный --local-port нужен только для запуска обоих "
                              "скриптов на одной машине в целях теста)")
    parser.add_argument("--peer-ip", default=DEFAULT_PEER_IP,
                         help="IP второй стороны напрямую (unicast), если broadcast "
                              "не проходит из-за настроек Wi-Fi (client isolation)")
    parser.add_argument("--broadcast-ip", default=DEFAULT_BROADCAST_IP,
                         help=f"адрес для broadcast-рассылки (по умолчанию {DEFAULT_BROADCAST_IP})")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL,
                         help="период heartbeat в секундах")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                         help="через сколько секунд без heartbeat партнёр считается offline")
    parser.add_argument("--no-chat", action="store_true",
                         help="не читать stdin (для запуска в фоне/через systemd)")
    parser.add_argument("--mcp-url", default=DEFAULT_MCP_URL,
                         help="URL MCP партнёра (напр. http://192.168.1.36:9092) — "
                              "включает дублирование heartbeat через shell_exec партнёра "
                              "на его 127.0.0.1, в обход внешнего файрвола/докер-сети")
    parser.add_argument("--mcp-timeout", type=float, default=DEFAULT_MCP_TIMEOUT,
                         help="таймаут HTTP-запроса к MCP, сек")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.local_port is None:
        args.local_port = args.port
    x, y = cell_to_xy(START_CELL)

    log.info("=== SVERK Dozor — узел связи: %s ===", ROLE.upper())
    log.info("Стартовая точка %s: клетка %s (x=%.2f м, y=%.2f м)", ROLE, START_CELL, x, y)
    log.info(
        "UDP-порт: %d | адрес отправки: %s | ожидаемый партнёр: %s",
        args.port, args.peer_ip or args.broadcast_ip, PEER_ROLE,
    )
    if not args.peer_ip:
        log.info(
            "Работаю в режиме broadcast. Если %s не выходит на связь — "
            "проверьте, что оба устройства в одной Wi-Fi сети без "
            "client isolation, либо запустите с --peer-ip <IP %s>.",
            PEER_ROLE, PEER_ROLE,
        )
    if args.mcp_url:
        log.info(
            "MCP-релей включён: %s (heartbeat дублируется через shell_exec "
            "партнёра на его 127.0.0.1:%d, в обход внешнего файрвола)",
            args.mcp_url, args.port,
        )

    link = DozorLink(args)
    link.start()
    try:
        if args.no_chat:
            while True:
                time.sleep(1.0)
        else:
            link._interactive_loop()
    except KeyboardInterrupt:
        print()  # переносим строку после ^C
    finally:
        log.info("Завершение работы, отправляю сообщение об отключении...")
        link.stop()
        log.info("Готово.")


if __name__ == "__main__":
    main()