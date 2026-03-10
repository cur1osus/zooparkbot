import tempfile
from pathlib import Path
from uuid import uuid4

import matplotlib.pyplot as plt
from cache import plot_cache
from db import User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import tools

PLOT_DIR = Path(tempfile.gettempdir()) / "zooparkbot_plots"
TOP_LIMIT = 10


async def remove_file_plot(pattern: str):
    PLOT_DIR.mkdir(parents=True, exist_ok=True)
    for file_path in PLOT_DIR.glob(pattern):
        if file_path.is_file():
            file_path.unlink()


async def get_users_with_animals(session: AsyncSession) -> list[User]:
    result = await session.scalars(select(User).where(User.animals != "{}"))
    return list(result.all())


def prepare_top_data(data: list[tuple[str, int]]) -> list[tuple[str, int]]:
    data.sort(key=lambda x: x[1], reverse=True)
    return data[:TOP_LIMIT][::-1]


async def get_top_income_data(session: AsyncSession):
    users = await get_users_with_animals(session=session)
    data = [
        (user.nickname, await tools.income_(session=session, user=user))
        for user in users
    ]
    return prepare_top_data(data)


async def get_top_referrals_data(session: AsyncSession):
    users = await get_users_with_animals(session=session)
    referrals_count = await tools.get_referrals_count_map(
        session=session,
        idpk_users=[user.idpk for user in users],
    )
    data = [(user.nickname, referrals_count.get(user.idpk, 0)) for user in users]
    return prepare_top_data(data)


async def get_top_animals_data(session: AsyncSession):
    users = await get_users_with_animals(session=session)
    data = [
        (user.nickname, await tools.get_total_number_animals(self=user))
        for user in users
    ]
    return prepare_top_data(data)


async def get_top_money_data(session: AsyncSession):
    users = await get_users_with_animals(session=session)
    data = [(user.nickname, int(user.usd)) for user in users]
    return prepare_top_data(data)


async def gen_plot(
    nicks: list[str],
    values: list[int],
    color: str,
    xlabel: str,
    ylabel: str,
    plot_type: str,
):
    PLOT_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 5))
    float_values = [float(value) for value in values]
    plt.grid(True, axis="x", linestyle="--", alpha=0.5)
    bars = ax.barh(nicks, float_values, color=color)
    ax.set(xlabel=xlabel, ylabel=ylabel, title="ТОП")
    max_width = max(float_values)
    for bar in bars:
        width = bar.get_width()
        if width > 0:
            is_small_value = (max_width // width) > 7
            label_x_pos = 0.01 * max_width if is_small_value else width / 2
            alignment = "left" if is_small_value else "center"
            label_color = "black" if is_small_value else "white"
        else:
            label_x_pos = 0.01 * max_width
            alignment = "left"
            label_color = "black"
        plt.text(
            label_x_pos,
            bar.get_y() + bar.get_height() / 2,
            f"{width:,.0f}",
            ha=alignment,
            va="center",
            color=label_color,
        )

    await remove_file_plot(pattern=f"plot_{plot_type}_*.png")
    filename = PLOT_DIR / f"plot_{plot_type}_{uuid4().hex}.png"
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    plt.close(fig)
    plot_cache[plot_type] = str(filename)
    return str(filename)


async def get_plot(session: AsyncSession, type: str):
    cached_path = plot_cache.get(type)
    if cached_path and Path(cached_path).exists():
        return cached_path

    config = {
        "income": ("royalblue", "Доход", "Игроки", get_top_income_data),
        "referrals": ("teal", "Рефералы", "Игроки", get_top_referrals_data),
        "animals": ("indianred", "Животные", "Игроки", get_top_animals_data),
        "money": ("darkgreen", "Доллары", "Игроки", get_top_money_data),
    }

    if type not in config:
        return None

    color, xlabel, ylabel, data_func = config[type]
    data = await data_func(session)
    if not data:
        return None

    nicks, values = zip(*data)
    return await gen_plot(
        nicks=list(nicks),
        values=list(values),
        color=color,
        xlabel=xlabel,
        ylabel=ylabel,
        plot_type=type,
    )
