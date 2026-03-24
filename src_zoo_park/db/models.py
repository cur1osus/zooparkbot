from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    Index,
    JSON,
    Numeric,
    String,
    Text as SQLText,
)
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
    info_about_items: Mapped[str] = mapped_column(SQLText, default="{}")
    current_unity: Mapped[str] = mapped_column(String(64), nullable=True)
    sub_on_chat: Mapped[bool] = mapped_column(default=False)
    sub_on_channel: Mapped[bool] = mapped_column(default=False)
    bonus: Mapped[int] = mapped_column(default=1)

    # Optimized fields for high-performance background jobs
    income_per_minute: Mapped[int] = mapped_column(Numeric(precision=65, scale=0), default=0, index=True)
    last_income_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class Unity(Base):
    __tablename__ = "unity"

    idpk_user: Mapped[int] = mapped_column()
    name: Mapped[str] = mapped_column(String(length=64))
    level: Mapped[int] = mapped_column(default=0)

    @property
    def format_name(self) -> str:
        return f"«{self.name}»"


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
    next_wake_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_wake_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
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
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class UserHistoryEvent(Base):
    __tablename__ = "user_history_events"
    __table_args__ = (
        Index("ix_user_history_events_user_time", "idpk_user", "event_time"),
        Index("ix_user_history_events_user_kind", "idpk_user", "event_kind"),
    )

    idpk_user: Mapped[int] = mapped_column(index=True)
    event_time: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, index=True
    )
    event_kind: Mapped[str] = mapped_column(
        String(length=32), default="message", index=True
    )
    source: Mapped[str | None] = mapped_column(String(length=32), nullable=True)
    event_text: Mapped[str | None] = mapped_column(
        SQLText().with_variant(MEDIUMTEXT(), "mysql"),
        nullable=True,
    )
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class UnityMember(Base):
    __tablename__ = "unity_members"
    __table_args__ = (
        Index(
            "ix_unity_members_unity_user_unique", "idpk_unity", "idpk_user", unique=True
        ),
        Index("ix_unity_members_user", "idpk_user"),
    )

    idpk_unity: Mapped[int] = mapped_column(index=True)
    idpk_user: Mapped[int] = mapped_column(index=True)
    role: Mapped[str] = mapped_column(String(length=16), default="member")
    joined_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, index=True
    )


class TransferMoneyClaim(Base):
    __tablename__ = "transfer_money_claims"
    __table_args__ = (
        Index(
            "ix_transfer_money_claims_transfer_user_unique",
            "idpk_transfer",
            "idpk_user",
            unique=True,
        ),
        Index("ix_transfer_money_claims_user", "idpk_user"),
    )

    idpk_transfer: Mapped[int] = mapped_column(index=True)
    idpk_user: Mapped[int] = mapped_column(index=True)
    claimed_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, index=True
    )


class UserAnimalState(Base):
    __tablename__ = "user_animal_states"
    __table_args__ = (
        Index(
            "ix_user_animal_states_user_code_unique",
            "idpk_user",
            "animal_code_name",
            unique=True,
        ),
        Index("ix_user_animal_states_code", "animal_code_name"),
    )

    idpk_user: Mapped[int] = mapped_column(index=True)
    animal_code_name: Mapped[str] = mapped_column(String(length=64), index=True)
    quantity: Mapped[int] = mapped_column(default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, index=True
    )


class UserAviaryState(Base):
    __tablename__ = "user_aviary_states"
    __table_args__ = (
        Index(
            "ix_user_aviary_states_user_code_unique",
            "idpk_user",
            "aviary_code_name",
            unique=True,
        ),
        Index("ix_user_aviary_states_code", "aviary_code_name"),
    )

    idpk_user: Mapped[int] = mapped_column(index=True)
    aviary_code_name: Mapped[str] = mapped_column(String(length=64), index=True)
    quantity: Mapped[int] = mapped_column(default=0)
    buy_count: Mapped[int] = mapped_column(default=0)
    current_price: Mapped[int] = mapped_column(BigInteger, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, index=True
    )


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
