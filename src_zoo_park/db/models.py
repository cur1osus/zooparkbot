from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Index, Numeric, String, Text as SQLText
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        Index(
            "ix_users_id_referrer_referral_verification",
            "id_referrer",
            "referral_verification",
        ),
    )

    id_user: Mapped[int] = mapped_column(BigInteger, unique=True)
    username: Mapped[str] = mapped_column(String(length=64), nullable=True, index=True)
    nickname: Mapped[str] = mapped_column(String(length=64), nullable=True)
    date_reg: Mapped[str] = mapped_column(DateTime)
    id_referrer: Mapped[int] = mapped_column(BigInteger, nullable=True, index=True)
    referral_verification: Mapped[bool] = mapped_column(default=False, index=True)
    moves: Mapped[int] = mapped_column(default=0)
    history_moves: Mapped[str] = mapped_column(
        SQLText().with_variant(MEDIUMTEXT(), "mysql"), default="{}"
    )
    paw_coins: Mapped[int] = mapped_column(default=0)
    amount_expenses_paw_coins: Mapped[int] = mapped_column(default=0)
    rub: Mapped[int] = mapped_column(Numeric(precision=65, scale=0), default=0)
    amount_expenses_rub: Mapped[int] = mapped_column(
        Numeric(precision=65, scale=0), default=0
    )
    usd: Mapped[int] = mapped_column(Numeric(precision=65, scale=0), default=0)
    amount_expenses_usd: Mapped[int] = mapped_column(
        Numeric(precision=65, scale=0), default=0
    )
    animals: Mapped[str] = mapped_column(SQLText, default="{}")
    info_about_items: Mapped[str] = mapped_column(SQLText, default="{}")
    aviaries: Mapped[str] = mapped_column(SQLText, default="{}")
    current_unity: Mapped[str] = mapped_column(String(64), nullable=True)
    sub_on_chat: Mapped[bool] = mapped_column(default=False)
    sub_on_channel: Mapped[bool] = mapped_column(default=False)
    bonus: Mapped[int] = mapped_column(default=1)


class Unity(Base):
    __tablename__ = "unity"

    idpk_user: Mapped[int] = mapped_column()
    name: Mapped[str] = mapped_column(String(length=64))
    members: Mapped[str] = mapped_column(SQLText, default="{}")
    level: Mapped[int] = mapped_column(default=0)

    @property
    def format_name(self) -> str:
        return f"«{self.name}»"

    def add_member(self, idpk_member: int, rule: str = "member") -> None:
        decoded_dict: dict = json.loads(self.members)
        decoded_dict[idpk_member] = rule
        self.members = json.dumps(decoded_dict, ensure_ascii=False)

    def remove_member(self, idpk_member: str) -> None:
        decoded_dict: dict = json.loads(self.members)
        if idpk_member in decoded_dict:
            del decoded_dict[idpk_member]
        self.members = json.dumps(decoded_dict, ensure_ascii=False)

    def remove_first_member(self) -> int:
        decoded_dict: dict = json.loads(self.members)
        key = 0
        if decoded_dict:
            key = list(decoded_dict.keys())[0]
            del decoded_dict[key]
        self.members = json.dumps(decoded_dict, ensure_ascii=False)
        return int(key)

    def get_number_members(self) -> int:
        """Возвращает количество участников вместе с владельцем"""
        decoded_dict: dict = json.loads(self.members)
        return len(decoded_dict) + 1

    def get_members_idpk(self) -> list[str]:
        decoded_dict: dict = json.loads(self.members)
        return list(decoded_dict.keys()) + [self.idpk_user]


class RequestToUnity(Base):
    __tablename__ = "requests_to_unity"

    idpk_user: Mapped[int] = mapped_column()
    idpk_unity_owner: Mapped[int] = mapped_column()
    date_request: Mapped[str] = mapped_column(DateTime)
    date_request_end: Mapped[str] = mapped_column(DateTime, index=True)


class NpcState(Base):
    __tablename__ = "npc_states"
    __table_args__ = (
        Index("ix_npc_states_idpk_user_unique", "idpk_user", unique=True),
        Index("ix_npc_states_next_wake_at", "next_wake_at"),
    )

    idpk_user: Mapped[int] = mapped_column(index=True)
    next_wake_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_wake_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_sleep_seconds: Mapped[int] = mapped_column(nullable=True)
    last_wake_source: Mapped[str] = mapped_column(String(length=32), nullable=True)
    last_wake_reason: Mapped[str] = mapped_column(String(length=255), nullable=True)


class NpcMemory(Base):
    __tablename__ = "npc_memory"
    __table_args__ = (
        Index("ix_npc_memory_user_kind_status", "idpk_user", "kind", "status"),
        Index("ix_npc_memory_user_importance", "idpk_user", "importance"),
        Index(
            "ix_npc_memory_user_kind_topic_unique",
            "idpk_user",
            "kind",
            "topic",
            unique=True,
        ),
    )

    idpk_user: Mapped[int] = mapped_column(index=True)
    kind: Mapped[str] = mapped_column(String(length=32), index=True)
    topic: Mapped[str] = mapped_column(String(length=128), index=True)
    payload: Mapped[str] = mapped_column(SQLText, default="{}")
    importance: Mapped[int] = mapped_column(default=0)
    confidence: Mapped[int] = mapped_column(default=0)
    status: Mapped[str] = mapped_column(String(length=32), default="active", index=True)
    access_count: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class Item(Base):
    __tablename__ = "items"

    id_item: Mapped[str] = mapped_column(SQLText)
    id_user: Mapped[int] = mapped_column(BigInteger)
    emoji: Mapped[str] = mapped_column(String(length=10))
    name: Mapped[str] = mapped_column(String(length=64))
    lvl: Mapped[int] = mapped_column(default=0)
    properties: Mapped[str] = mapped_column(SQLText)
    rarity: Mapped[str] = mapped_column(String(length=64))
    is_active: Mapped[bool] = mapped_column(default=False)

    @property
    def name_with_emoji(self) -> str:
        return f"{self.name} {self.emoji}"


class Animal(Base):
    __tablename__ = "animals"

    code_name: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String(length=64))
    description: Mapped[str] = mapped_column(String(length=4096))
    price: Mapped[int] = mapped_column(BigInteger)
    income: Mapped[int] = mapped_column(BigInteger)


class Aviary(Base):
    __tablename__ = "aviaries"

    name: Mapped[str] = mapped_column(String(length=64))
    code_name: Mapped[str] = mapped_column(String(length=64))
    size: Mapped[int] = mapped_column()
    price: Mapped[int] = mapped_column()

    @property
    def name_with_size(self) -> str:
        return f"{self.name} [{self.size}]"


class RandomMerchant(Base):
    __tablename__ = "random_merchants"

    id_user: Mapped[int] = mapped_column(BigInteger)
    name: Mapped[str] = mapped_column(String(length=64))
    code_name_animal: Mapped[str] = mapped_column(String(length=64))
    discount: Mapped[int] = mapped_column()
    price_with_discount: Mapped[int] = mapped_column()
    quantity_animals: Mapped[int] = mapped_column()
    price: Mapped[int] = mapped_column()
    first_offer_bought: Mapped[bool] = mapped_column(default=False)


class TransferMoney(Base):
    __tablename__ = "transfer_money"

    id_transfer: Mapped[str] = mapped_column(String(length=10), index=True)
    idpk_user: Mapped[int] = mapped_column()
    currency: Mapped[str] = mapped_column(String(length=10))
    one_piece_sum: Mapped[int] = mapped_column(BigInteger)
    pieces: Mapped[int] = mapped_column()
    used: Mapped[str] = mapped_column(SQLText, nullable=True)
    id_mess: Mapped[str] = mapped_column(String(length=80), nullable=True)
    source_chat_id: Mapped[int] = mapped_column(BigInteger, nullable=True)
    status: Mapped[bool] = mapped_column(default=False)


class Donate(Base):
    __tablename__ = "donates"

    idpk_user: Mapped[int] = mapped_column()
    amount: Mapped[int] = mapped_column()
    refund_id: Mapped[str] = mapped_column(String(length=200), nullable=True)


class Game(Base):
    __tablename__ = "games"
    __table_args__ = (
        Index("ix_games_end_last_update_mess", "end", "last_update_mess"),
    )

    id_game: Mapped[str] = mapped_column(String(length=20), index=True)
    idpk_user: Mapped[int] = mapped_column()
    type_game: Mapped[str] = mapped_column(String(length=64))
    amount_gamers: Mapped[int] = mapped_column()
    amount_award: Mapped[int] = mapped_column(Numeric(precision=65, scale=0))
    currency_award: Mapped[str] = mapped_column(String(length=10))
    amount_moves: Mapped[int] = mapped_column(default=7)
    id_mess: Mapped[str] = mapped_column(String(length=64), nullable=True)
    source_chat_id: Mapped[int] = mapped_column(BigInteger, nullable=True)
    activate: Mapped[bool] = mapped_column(default=False)
    end: Mapped[bool] = mapped_column(default=False)
    end_date: Mapped[str] = mapped_column(DateTime)
    last_update_mess: Mapped[bool] = mapped_column(default=False)


class Gamer(Base):
    __tablename__ = "gamers"

    id_game: Mapped[str] = mapped_column(String(length=20))
    idpk_gamer: Mapped[int] = mapped_column()
    moves: Mapped[int] = mapped_column()
    score: Mapped[int] = mapped_column(default=0)
    game_end: Mapped[bool] = mapped_column(default=False)


class MessageToSupport(Base):
    __tablename__ = "messages_to_support"

    id_message: Mapped[int] = mapped_column(nullable=True)
    idpk_user: Mapped[int] = mapped_column()
    question: Mapped[str] = mapped_column(SQLText)
    id_message_question: Mapped[int] = mapped_column()
    photo_id: Mapped[str] = mapped_column(String(length=200), nullable=True)
    id_message_answer: Mapped[int] = mapped_column(nullable=True)


class Text(Base):
    __tablename__ = "texts"

    name: Mapped[str] = mapped_column(String(length=100), index=True)  # Название текста
    text: Mapped[str] = mapped_column(
        String(length=4096), default="текст не задан"
    )  # Текст


class Button(Base):
    __tablename__ = "buttons"

    name: Mapped[str] = mapped_column(String(length=100), index=True)  # Название кнопки
    text: Mapped[str] = mapped_column(
        String(length=64), default="кнопка"
    )  # Текст кнопки


class BlackList(Base):
    __tablename__ = "blacklist"

    id_user: Mapped[int] = mapped_column(BigInteger)  # Идентификатор пользователя


class Value(Base):
    __tablename__ = "values"

    name: Mapped[str] = mapped_column(
        String(length=100), index=True
    )  # Название значения
    value_int: Mapped[int] = mapped_column(BigInteger, default=0)  # Значение целое
    value_str: Mapped[str] = mapped_column(
        String(length=4096), default="не установлено"
    )  # Значение строка


class Photo(Base):
    __tablename__ = "photos"

    name: Mapped[str] = mapped_column(String(length=30), index=True)  # Название фото
    photo_id: Mapped[str] = mapped_column(String(length=100))  # Идентификатор фото
