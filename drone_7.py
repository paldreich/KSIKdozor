#!/usr/bin/env python3
"""
Миссия: взлёт с A6 → E2 (VLM) → A1 (VLM) → A6 → посадка.

Маршрут по aruco_map (0.80 м/клетка).
Origin aruco_map — центр метки D1 (как на /aruco/vis/map_debug_image/plane_1).

На точках E2 и A1 — VLM-сканирование (a toy).
Полёт между точками на TAKEOFF_Z_M (1.5 м); перед сканом снижение на VLM_SCAN_Z_M (1.2 м),
после скана — возврат на TAKEOFF_Z_M.
LED: зелёный = found, красный = not found.

Предусловия на реальном дроне:
  - Полный стек Обрика, markers.txt (origin в центре D1).
  - ArUco-навигация включена (VIO в PX4).
  - VLM action server: ros2 action list | grep vlm
  - Рекомендуется: ros2 param set /vlm_action_server max_tokens 8

Запуск на борту (SSH):

  source /opt/ros/humble/setup.bash
  source ~/sverk_ws/install/setup.bash
  python3 ~/drone_7.py

Скопировать с ноутбука на дрон:
  scp missions/vlm_query.py missions/drone_7.py sverk@<IP_дрона>:~/
"""

from __future__ import annotations

import math
import re
import time

import sverk_interfaces
from vlm_query import ask_vlm_bool

# =============================================================================
# НАСТРОЙКИ — меняйте здесь
# =============================================================================

START_CELL = "A6"          # физический старт и посадка
VLM_SUBJECT = "a toy"      # объект для VLM (как в vlm_query.py)

# (клетка, сканировать VLM после прилёта)
WAYPOINTS: list[tuple[str, bool]] = [
    ("E2", True),   # VLM: ожидаем found=True
    ("A1", True),   # VLM: ожидаем found=False
    ("F6", False),  # посадка без скана
]

ORIGIN_ROW = "D"           # центр метки D1 = (0, 0) в aruco_map
CELL_SIZE_M = 0.80         # 0.80 м/клетка
TAKEOFF_Z_M = 1.5          # высота полёта между точками, м
VLM_SCAN_Z_M = 1.2         # высота только на время VLM-скана, м
FLIGHT_SPEED = 0.3         # скорость, м/с
FRAME_ID = "aruco_map"
TOLERANCE = 0.25           # допустимое расстояние до цели, м
TIMEOUT = 60.0             # максимальное время ожидания прилёта, с
LOC_WAIT_TIMEOUT = 30.0    # ждать подключения PX4 перед взлётом, с
ARUCO_WAIT_TIMEOUT = 30.0  # ждать TF aruco_map после взлёта, с
SETTLE_AFTER_TAKEOFF_S = 2.0  # пауза после взлёта перед проверкой ArUco
SETTLE_AFTER_ARRIVAL_S = 3.0  # пауза после прилёта перед VLM/следующим этапом

_CELL_RE = re.compile(r"^[A-Fa-f][1-6]$")


# =============================================================================
# КООРДИНАТЫ СЕТКИ (aruco_map, origin — центр D1)
# =============================================================================

def _validate_cell(cell: str, name: str) -> str:
    cell = cell.strip().upper()
    if not _CELL_RE.match(cell):
        raise ValueError(
            f"{name}='{cell}' — неверный формат. Ожидается буква A–F и цифра 1–6, например 'A2'."
        )
    return cell


def cell_to_aruco_map(cell: str) -> tuple[float, float]:
    """
    Центр клетки в aruco_map (м).

    Origin — центр метки D1 (не нижний левый угол).
    X вправо, Y вверх (F→A).
    row_from_origin: D=1, E=0, F=-1, C=2, B=3, A=4.
    """
    cell = _validate_cell(cell, "cell")
    row, col = cell[0], int(cell[1])
    row_from_origin = ord(ORIGIN_ROW) - ord(row) + 1
    x = (col - 1) * CELL_SIZE_M
    y = (row_from_origin - 1) * CELL_SIZE_M
    return x, y


def _wait_for_px4(drone, timeout: float) -> object:
    """Ждёт подключения PX4 (frame map, только connected)."""
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None

    while time.monotonic() < deadline:
        try:
            t = drone.control.get_telemetry(frame_id="map", timeout=2.0)
            if t.connected:
                return t
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(0.5)

    msg = f"PX4 не подключён за {timeout:.0f} с."
    if last_err is not None:
        msg += f" Последняя ошибка: {last_err}"
    raise TimeoutError(msg)


def _wait_for_aruco_localization(drone, timeout: float) -> object:
    """Ждёт валидную позицию в aruco_map (finite x, y, z)."""
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None

    while time.monotonic() < deadline:
        try:
            t = drone.control.get_telemetry(frame_id=FRAME_ID, timeout=2.0)
            if t.connected and all(math.isfinite(v) for v in (t.x, t.y, t.z)):
                return t
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(0.5)

    msg = (
        f"ArUco-локализация не готова за {timeout:.0f} с "
        f"(проверьте: ros2 topic hz /aruco/world_pose)."
    )
    if last_err is not None:
        msg += f" Последняя ошибка: {last_err}"
    raise TimeoutError(msg)


def _fly_to_cell(drone, cell: str, yaw: float) -> None:
    x, y = cell_to_aruco_map(cell)
    print(f"Полёт к {cell} ({x:.3f}, {y:.3f})...")
    drone.control.navigate_wait(
        x=x,
        y=y,
        z=TAKEOFF_Z_M,
        yaw=yaw,
        speed=FLIGHT_SPEED,
        frame_id=FRAME_ID,
        tolerance=TOLERANCE,
        timeout=TIMEOUT,
    )
    if SETTLE_AFTER_ARRIVAL_S > 0:
        print(f"Стабилизация {SETTLE_AFTER_ARRIVAL_S:.0f} с...")
        time.sleep(SETTLE_AFTER_ARRIVAL_S)
    print(f"Прилетели в {cell}.")


def _set_altitude(drone, x: float, y: float, z: float, yaw: float) -> None:
    print(f"Смена высоты на z={z:.2f} м...")
    drone.control.navigate_wait(
        x=x,
        y=y,
        z=z,
        yaw=yaw,
        speed=FLIGHT_SPEED,
        frame_id=FRAME_ID,
        tolerance=TOLERANCE,
        timeout=TIMEOUT,
    )


def _scan_vlm_at_waypoint(drone, cell: str, yaw: float) -> bool:
    x, y = cell_to_aruco_map(cell)
    if VLM_SCAN_Z_M != TAKEOFF_Z_M:
        _set_altitude(drone, x, y, VLM_SCAN_Z_M, yaw)
        if SETTLE_AFTER_ARRIVAL_S > 0:
            print(f"Стабилизация {SETTLE_AFTER_ARRIVAL_S:.0f} с на высоте скана...")
            time.sleep(SETTLE_AFTER_ARRIVAL_S)
    print(f"VLM сканирование в {cell} (subject={VLM_SUBJECT!r}, z={VLM_SCAN_Z_M:.2f} м)...")
    found = ask_vlm_bool(drone, subject=VLM_SUBJECT, use_npu_vision=True)
    print(f"Результат {cell}: found={found}")
    if VLM_SCAN_Z_M != TAKEOFF_Z_M:
        _set_altitude(drone, x, y, TAKEOFF_Z_M, yaw)
    return found


# =============================================================================
# МИССИЯ
# =============================================================================

def main() -> None:
    start = _validate_cell(START_CELL, "START_CELL")
    waypoints = [
        (_validate_cell(cell, f"WAYPOINTS[{i}]"), do_scan)
        for i, (cell, do_scan) in enumerate(WAYPOINTS)
    ]

    print(f"Старт: {start} (origin aruco_map: центр {ORIGIN_ROW}1)")
    route_str = " → ".join(
        f"{cell}{' [VLM]' if scan else ''}" for cell, scan in waypoints
    )
    print(f"Маршрут: {route_str}")

    drone = sverk_interfaces.init(Nodename="drone_7")
    try:
        print(f"Ожидаю PX4 (до {LOC_WAIT_TIMEOUT:.0f} с)...")
        telemetry = _wait_for_px4(drone, LOC_WAIT_TIMEOUT)
        yaw = telemetry.yaw if math.isfinite(telemetry.yaw) else 0.0
        print(f"PX4 готов. yaw={yaw:.3f} рад")

        print(f"Взлёт на {TAKEOFF_Z_M} м (frame_id=body)...")
        drone.control.navigate_wait(
            x=0.0,
            y=0.0,
            z=TAKEOFF_Z_M,
            yaw=0.0,
            speed=FLIGHT_SPEED,
            frame_id="body",
            auto_arm=True,
            tolerance=TOLERANCE,
            timeout=TIMEOUT,
        )
        print("Взлетели.")

        if SETTLE_AFTER_TAKEOFF_S > 0:
            print(f"Пауза {SETTLE_AFTER_TAKEOFF_S:.1f} с перед проверкой ArUco...")
            time.sleep(SETTLE_AFTER_TAKEOFF_S)

        print(f"Ожидаю ArUco-локализацию (до {ARUCO_WAIT_TIMEOUT:.0f} с)...")
        pose = _wait_for_aruco_localization(drone, ARUCO_WAIT_TIMEOUT)
        print(
            f"ArUco готов: x={pose.x:.3f}, y={pose.y:.3f}, z={pose.z:.3f} "
            f"(frame_id={FRAME_ID})"
        )

        for cell, do_scan in waypoints:
            _fly_to_cell(drone, cell, yaw)
            if do_scan:
                _scan_vlm_at_waypoint(drone, cell, yaw)

        print("Посадка...")
        land_resp = drone.control.land(timeout=20.0)
        print(f"Посадка: success={land_resp.success}, msg={land_resp.message}")
    finally:
        drone.close()


if __name__ == "__main__":
    main()
