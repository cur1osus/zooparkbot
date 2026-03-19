import tempfile
from dataclasses import dataclass
from heapq import nlargest
from pathlib import Path
from typing import Awaitable, Callable
from uuid import uuid4

import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
from matplotlib.ticker import FuncFormatter, MaxNLocator
from cache import plot_cache
from db import User, UserAnimalState
from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

import tools

PLOT_DIR = Path(tempfile.gettempdir()) / "zooparkbot_plots"
TOP_LIMIT = 10
FIGURE_SIZE = (12.5, 6.5)

THEME = {
    "figure_bg": "#F5EFE2",
    "axes_bg": "#FFF9EF",
    "grid": "#D8C9AE",
    "text": "#2F241F",
    "muted": "#7A685C",
    "border": "#D9C2A3",
    "value_bg": "#FFFDF8",
    "leader": "#D9A441",
}


@dataclass(frozen=True, slots=True)
class PlotSpec:
    accent: str
    title: str
    subtitle: str
    xlabel: str
    ylabel: str
    data_loader: Callable[[AsyncSession], Awaitable[list[tuple[str, int]]]]


def ensure_plot_dir() -> None:
    PLOT_DIR.mkdir(parents=True, exist_ok=True)


def remove_plot_files(pattern: str) -> None:
    ensure_plot_dir()
    for file_path in PLOT_DIR.glob(pattern):
        if file_path.is_file():
            file_path.unlink()


async def get_users_with_animals(session: AsyncSession) -> list[User]:
    animals_exists = exists(
        select(UserAnimalState.idpk).where(UserAnimalState.idpk_user == User.idpk)
    )
    result = await session.scalars(select(User).where(animals_exists))
    return list(result.all())


def prepare_top_data(data: list[tuple[str, int]]) -> list[tuple[str, int]]:
    top_data = nlargest(TOP_LIMIT, data, key=lambda item: item[1])
    top_data.reverse()
    return top_data


async def build_user_top_data(
    session: AsyncSession,
    metric_loader: Callable[[AsyncSession, User], Awaitable[int]],
) -> list[tuple[str, int]]:
    users = await get_users_with_animals(session=session)
    data: list[tuple[str, int]] = []

    for user in users:
        value = await metric_loader(session, user)
        data.append((user.nickname, int(value)))

    return prepare_top_data(data)


def build_local_top_data(
    users: list[User], metric_loader: Callable[[User], int]
) -> list[tuple[str, int]]:
    return prepare_top_data(
        [(user.nickname, int(metric_loader(user))) for user in users]
    )


async def get_user_income(session: AsyncSession, user: User) -> int:
    return await tools.income_(session=session, user=user)


async def get_user_animals(session: AsyncSession, user: User) -> int:
    return sum(await tools.get_numbers_animals(self=user, session=session))


def get_user_money(user: User) -> int:
    return int(user.usd)


async def get_top_income_data(session: AsyncSession) -> list[tuple[str, int]]:
    return await build_user_top_data(session=session, metric_loader=get_user_income)


async def get_top_animals_data(session: AsyncSession) -> list[tuple[str, int]]:
    return await build_user_top_data(session=session, metric_loader=get_user_animals)


async def get_top_referrals_data(session: AsyncSession) -> list[tuple[str, int]]:
    users = await get_users_with_animals(session=session)
    referrals_count = await tools.get_referrals_count_map(
        session=session,
        idpk_users=[user.idpk for user in users],
    )
    data = [(user.nickname, referrals_count.get(user.idpk, 0)) for user in users]
    return prepare_top_data(data)


async def get_top_animals_data(session: AsyncSession) -> list[tuple[str, int]]:
    users = await get_users_with_animals(session=session)
    return build_local_top_data(users=users, metric_loader=get_user_animals)


async def get_top_money_data(session: AsyncSession) -> list[tuple[str, int]]:
    users = await get_users_with_animals(session=session)
    return build_local_top_data(users=users, metric_loader=get_user_money)


PLOT_SPECS = {
    "income": PlotSpec(
        accent="#4A7BD1",
        title="Топ по доходу",
        subtitle="Лучшие игроки зоопарка по пассивному доходу",
        xlabel="Доход",
        ylabel="Игроки",
        data_loader=get_top_income_data,
    ),
    "referrals": PlotSpec(
        accent="#2F9D8F",
        title="Топ по рефералам",
        subtitle="Самые активные игроки по приглашениям",
        xlabel="Рефералы",
        ylabel="Игроки",
        data_loader=get_top_referrals_data,
    ),
    "animals": PlotSpec(
        accent="#D96D4D",
        title="Топ по животным",
        subtitle="Коллекционеры с самым большим зоопарком",
        xlabel="Животные",
        ylabel="Игроки",
        data_loader=get_top_animals_data,
    ),
    "money": PlotSpec(
        accent="#4D8F4B",
        title="Топ по долларам",
        subtitle="Самые обеспеченные владельцы зоопарка",
        xlabel="Доллары",
        ylabel="Игроки",
        data_loader=get_top_money_data,
    ),
}


def format_nickname(nickname: str, max_length: int = 18) -> str:
    if len(nickname) <= max_length:
        return nickname
    return f"{nickname[: max_length - 3]}..."


def format_value(value: float, _: int) -> str:
    return f"{value:,.0f}".replace(",", " ")


def build_bar_colors(
    accent: str, amount: int
) -> list[tuple[float, float, float, float]]:
    base_rgb = mcolors.to_rgb(accent)
    colors = []

    for index in range(amount):
        alpha = 0.35 + (0.45 * (index + 1) / max(amount, 1))
        colors.append((*base_rgb, alpha))

    if colors:
        colors[-1] = mcolors.to_rgba(THEME["leader"])

    return colors


def apply_axes_style(ax, spec: PlotSpec, max_width: float) -> None:
    ax.set_facecolor(THEME["axes_bg"])
    ax.grid(
        True,
        axis="x",
        linestyle=(0, (4, 4)),
        linewidth=0.9,
        color=THEME["grid"],
        zorder=0,
    )
    ax.set_xlabel(spec.xlabel, fontsize=12, color=THEME["muted"], labelpad=12)
    ax.set_ylabel(spec.ylabel, fontsize=12, color=THEME["muted"], labelpad=12)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=6, integer=True))
    ax.xaxis.set_major_formatter(FuncFormatter(format_value))
    ax.tick_params(axis="x", colors=THEME["muted"], labelsize=11)
    ax.tick_params(axis="y", colors=THEME["text"], labelsize=12, length=0)
    ax.set_xlim(0, max_width * 1.18 if max_width else 1)

    for spine_name in ("top", "right"):
        ax.spines[spine_name].set_visible(False)

    ax.spines["left"].set_color(THEME["border"])
    ax.spines["bottom"].set_color(THEME["border"])


def add_value_labels(ax, bars, max_width: float) -> None:
    for bar in bars:
        width = bar.get_width()
        label_x_pos = width + max_width * 0.02
        ax.text(
            label_x_pos,
            bar.get_y() + bar.get_height() / 2,
            format_value(width, 0),
            ha="left",
            va="center",
            color=THEME["text"],
            fontsize=11,
            fontweight="bold",
            bbox={
                "boxstyle": "round,pad=0.28",
                "facecolor": THEME["value_bg"],
                "edgecolor": THEME["border"],
                "linewidth": 1,
            },
        )


def add_plot_heading(fig, spec: PlotSpec, left: float) -> None:
    fig.text(
        left,
        0.965,
        spec.title,
        fontsize=22,
        fontweight="bold",
        color=THEME["text"],
        ha="left",
        va="top",
    )
    fig.text(
        left,
        0.915,
        spec.subtitle,
        fontsize=11,
        color=THEME["muted"],
        ha="left",
        va="top",
    )


def render_plot(
    nicks: list[str],
    values: list[int],
    spec: PlotSpec,
    plot_type: str,
) -> str:
    plot_left = 0.22
    plot_right = 0.94
    plot_top = 0.82
    plot_bottom = 0.16

    ensure_plot_dir()
    fig, ax = plt.subplots(figsize=FIGURE_SIZE)
    fig.patch.set_facecolor(THEME["figure_bg"])

    display_nicks = [format_nickname(nick) for nick in nicks]
    float_values = [float(value) for value in values]
    max_width = max(float_values, default=1)
    bar_colors = build_bar_colors(spec.accent, len(float_values))

    apply_axes_style(ax=ax, spec=spec, max_width=max_width)
    bars = ax.barh(
        display_nicks,
        float_values,
        color=bar_colors,
        edgecolor=THEME["border"],
        linewidth=1.2,
        height=0.62,
        zorder=3,
    )
    add_value_labels(ax=ax, bars=bars, max_width=max_width)
    add_plot_heading(fig=fig, spec=spec, left=plot_left)
    fig.subplots_adjust(
        left=plot_left,
        right=plot_right,
        top=plot_top,
        bottom=plot_bottom,
    )

    remove_plot_files(pattern=f"plot_{plot_type}_*.png")
    filename = PLOT_DIR / f"plot_{plot_type}_{uuid4().hex}.png"
    plt.savefig(filename, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(filename)


async def get_plot(session: AsyncSession, type: str):
    cached_path = plot_cache.get(type)
    if cached_path and Path(cached_path).exists():
        return cached_path

    spec = PLOT_SPECS.get(type)
    if spec is None:
        return None

    data = await spec.data_loader(session)
    if not data:
        return None

    nicks, values = zip(*data)
    filename = render_plot(
        nicks=list(nicks),
        values=list(values),
        spec=spec,
        plot_type=type,
    )
    plot_cache[type] = filename
    return filename
