import json
import random
import re
import string
from datetime import datetime, timedelta

from db import User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import tools


def gen_key(length):
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(length)
    )


multipliers = {"k": 10**3, "к": 10**3, "m": 10**6, "м": 10**6, "b": 10**9, "б": 10**9}


async def remove_punctuation_and_spaces(text: str) -> str:
    # Регулярное выражение для замены всех знаков и пробелов на пустую строку
    return re.sub(r"[^\w]", "", text)


async def find_integers(text: str) -> int | None:
    num: str = await remove_punctuation_and_spaces(text)

    if not num:
        return None
    elif num.isdigit():
        return int(num)
    elif num.isalpha():
        return None

    match_num = re.search(r"\d+", num)

    number = int(match_num.group())
    num = num[match_num.end() :]
    suffix = re.search(r"[^\W\d_]+", num)

    suffix = (suffix.group()).lower()

    if len(suffix) > 6:
        return None

    for s in suffix:
        if s not in multipliers:
            continue
        number *= multipliers[s]
    return int(number)


async def fetch_and_parse_str_value(
    session: AsyncSession,
    value_name: str,
    func_to_element=int,
    sep: str = ",",
) -> list[any]:
    value_str = await tools.get_value(
        session=session,
        value_name=value_name,
        value_type="str",
    )
    return [func_to_element(v.strip()) for v in value_str.split(sep)]


async def get_events_list(session: AsyncSession, id_user: int):
    events_list = []
    events = await session.execute(
        select(User.id_user, User.nickname, User.history_moves).where(
            User.id_user != id_user,
            User.history_moves != "{}",
        )
    )
    for id_user, nickname, history_moves in events.all():
        d: dict = json.loads(history_moves)
        mention_user = tools.mention_html(id_user, nickname)
        events_list.append((d.items(), mention_user))
    return events_list


MESSAGE_LENGTH = 4096


def sort_events_by_time(events_list: list[tuple], time: int):
    combined_events = []

    # Пороговое время, до которого нужно отобрать события
    start_time = datetime.now() - timedelta(minutes=time)

    # Объединяем все словари в один и обрабатываем коллизии
    for events, mention_user in events_list:
        for str_time, event in events:
            # Преобразуем строку временной метки в объект datetime
            t_time = datetime.strptime(str_time, "%d.%m.%Y %H:%M:%S.%f")
            # Фильтруем события по временному промежутку
            if t_time < start_time:
                continue
            combined_events.append(
                [t_time.strftime("%d/%m/%Y %H:%M:%S.%f"), f"{event} | {mention_user}"]
            )
    # Сортируем объединённый словарь по ключам (временным меткам)
    sorted_events = sorted(
        combined_events, key=lambda x: datetime.strptime(x[0], "%d/%m/%Y %H:%M:%S.%f")
    )

    # Формируем текст на основе отсортированных событий
    text_container = []
    text = ""
    for str_time, event in sorted_events:
        _ = str_time.split(".")[0]
        str_time = _.split(" ")[1]
        text_to_append = f"{str_time.split('.')[0]}: {event}\n"
        if len(text) + len(text_to_append) > MESSAGE_LENGTH:
            text_container.append(text)
            text = ""
        text += text_to_append
    text_container.append(text) if text else None

    return text_container
