"""
vlm_query.py

Обращение к уже запущенной на дроне VLM-модели через ROS 2 action
/vlm/query (см. CHEATSHEET.md, раздел "VLM"). Ничего дополнительно
разворачивать не нужно -- модель уже крутится в образе.

Использование:
    import sverk_interfaces
    from vlm_query import ask_vlm

    drone = sverk_interfaces.init(Nodename="mission")
    result = ask_vlm(
        drone,
        query='Is there a brown toy animal with large round ears in this image? '
              'Answer only JSON: {"found": true|false, "confidence": 0..1}',
        use_npu_vision=True,
    )
    print(result)  # {"found": True, "confidence": 0.9}
"""

import json
import re

import rclpy
from rclpy.action import ActionClient
from vlm_interfaces.action import VlmQuery
from led_interfaces.srv import SetLEDEffect


def _log(msg: str):
    print(f"[vlm_query] {msg}", flush=True)


def ask_vlm(drone, query: str, use_npu_vision: bool = True,
            timeout_sec: float = 10.0, wait_for_result_sec: float = 90.0):
    """
    Делает снимок текущей камеры дрона и отправляет его вместе с промптом
    в уже работающий VLM action-сервер. Промпт -- ТОЛЬКО на английском
    (см. шпаргалку).

    Возвращает распарсенный dict, если модель ответила валидным JSON,
    иначе -- сырую строку ответа.
    """
    _log("Делаю снимок с камеры дрона...")
    img = drone.image.take_picture(raw=True)
    _log("Снимок готов.")

    _log("Создаю action-клиент /vlm/query...")
    client = ActionClient(drone.node, VlmQuery, "/vlm/query")

    _log(f"Жду сервер /vlm/query (timeout={timeout_sec}s)...")
    if not client.wait_for_server(timeout_sec=timeout_sec):
        raise RuntimeError(
            "VLM action server /vlm/query недоступен. "
            "Проверь: ros2 action list | grep vlm"
        )
    _log("Сервер найден.")

    goal = VlmQuery.Goal()
    goal.image = img
    goal.query = query
    goal.use_npu_vision = use_npu_vision

    _log(f"Отправляю goal (use_npu_vision={use_npu_vision}, query={query!r})...")
    fut = client.send_goal_async(goal)
    rclpy.spin_until_future_complete(drone.node, fut, timeout_sec=wait_for_result_sec)

    if not fut.done():
        raise RuntimeError(
            f"Не дождался принятия goal за {wait_for_result_sec}s -- "
            "сервер завис или недоступен."
        )

    goal_handle = fut.result()
    if goal_handle is None or not goal_handle.accepted:
        raise RuntimeError("VLM action не был принят сервером")
    _log("Goal принят сервером, жду результат (это может занять время)...")

    result_fut = goal_handle.get_result_async()
    rclpy.spin_until_future_complete(drone.node, result_fut, timeout_sec=wait_for_result_sec)

    if not result_fut.done():
        raise RuntimeError(
            f"Не дождался результата VLM за {wait_for_result_sec}s -- "
            "инференс идёт слишком долго или сервер завис."
        )

    res = result_fut.result().result
    _log(f"Получен ответ (inference_time_ms={getattr(res, 'inference_time_ms', '?')}): "
         f"{res.response!r}")
    # res.response  res.inference_time_ms  res.success

    if not res.success:
        raise RuntimeError(f"VLM инференс не удался: {res.response}")

    match = re.search(r"\{.*\}", res.response, re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group(0))
            _log(f"Ответ распарсен как JSON: {parsed}")
            return parsed
        except json.JSONDecodeError:
            _log("Похоже на JSON, но распарсить не удалось -- отдаю сырой текст.")

    _log("Ответ не JSON -- отдаю сырой текст.")
    return res.response  # не JSON -- отдаём как есть


def set_led_effect(drone, effect: str, r: int, g: int, b: int, timeout_sec: float = 5.0):
    """Прямой вызов сервиса /led_control/set_effect через rclpy,
    в обход drone.led (в текущей версии sverk_interfaces он ходит на
    несуществующий /led/set_effect -- версия библиотеки не совпадает
    с запущенной LED-нодой)."""
    client = drone.node.create_client(SetLEDEffect, "/led_control/set_effect")

    if not client.wait_for_service(timeout_sec=timeout_sec):
        raise RuntimeError(
            "Сервис /led_control/set_effect недоступен. "
            "Проверь: ros2 service list | grep led"
        )

    req = SetLEDEffect.Request()
    req.effect = effect
    req.r = r
    req.g = g
    req.b = b

    fut = client.call_async(req)
    rclpy.spin_until_future_complete(drone.node, fut, timeout_sec=timeout_sec)

    if not fut.done():
        raise RuntimeError(f"Не дождался ответа от /led_control/set_effect за {timeout_sec}s")

    return fut.result()


def apply_led_reaction(drone, found: bool):
    """found=True -> зелёная лента, found=False -> красная."""
    try:
        if found:
            _log("found=True -> LED зелёный")
            set_led_effect(drone, "fill", r=0, g=255, b=0)
        else:
            _log("found=False -> LED красный")
            set_led_effect(drone, "fill", r=255, g=0, b=0)
    except RuntimeError as e:
        _log(f"Не удалось выставить LED: {e}")


def _parse_yes_no(text: str) -> bool:
    """Маленькие VLM часто игнорируют просьбу ответить JSON и просто
    что-то описывают -- поэтому ищем yes/no/true/false по тексту,
    а не парсим строгий JSON."""
    t = text.strip().lower()
    # сначала смотрим на самое начало ответа -- туда модель обычно
    # ставит короткий вердикт, даже если потом уходит в описание
    head = t[:20]
    if "yes" in head or "true" in head:
        return True
    if "no" in head or "false" in head:
        return False
    # иначе ищем по всему тексту
    if "yes" in t or "true" in t:
        return True
    return False


def ask_vlm_bool(drone, subject: str, use_npu_vision: bool = True, **kwargs) -> bool:
    """
    Строгий yes/no запрос -- надёжнее, чем просить JSON у маленькой модели.
    subject: что искать, например "a toy" или "a red balloon".
    Сразу включает LED-реакцию (зелёный/красный) и возвращает bool.
    """
    query = (
        f"Do you see {subject} in this image? "
        "Answer with a single word only: yes or no."
    )
    raw = ask_vlm(drone, query, use_npu_vision=use_npu_vision, **kwargs)

    text = raw if isinstance(raw, str) else json.dumps(raw)
    found = _parse_yes_no(text)
    _log(f"Распарсено как: {found}")

    apply_led_reaction(drone, found)
    return found


def ask_vlm_with_led(drone, query: str, use_npu_vision: bool = True, **kwargs):
    """Как ask_vlm, но дополнительно сразу включает LED-реакцию по полю "found"
    в ответе модели. Если ответ не JSON или в нём нет "found" -- LED не трогаем."""
    result = ask_vlm(drone, query, use_npu_vision=use_npu_vision, **kwargs)

    if isinstance(result, dict) and "found" in result:
        apply_led_reaction(drone, bool(result["found"]))
    else:
        _log("В ответе нет поля 'found' -- LED не трогаю.")

    return result


if __name__ == "__main__":
    import sverk_interfaces

    _log("Инициализирую sverk_interfaces...")
    drone = sverk_interfaces.init(Nodename="vlm_test")
    _log("Инициализация завершена.")
    try:
        # max_tokens лучше заранее ограничить снаружи, чтобы модель не
        # генерировала длинные описания и отвечала быстрее:
        #   ros2 param set /vlm_action_server max_tokens 8
        found = ask_vlm_bool(drone, subject="a toy", use_npu_vision=True)
        _log(f"Финальный результат: found={found}")
        print(found)
    finally:
        _log("Закрываю соединение с дроном...")
        drone.close()
        _log("Готово.")
