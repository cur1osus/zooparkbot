from __future__ import annotations

import abc
from typing import Any, Generic, TypeVar, TYPE_CHECKING
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from db import User
    from ..client import NpcDecisionClient

T = TypeVar("T", bound=BaseModel)

class ActionContext(BaseModel):
    """Контекст выполнения действия NPC."""
    session: Any  # AsyncSession
    user: Any     # User
    observation: dict[str, Any]
    client: Any | None = None # NpcDecisionClient
    
    class Config:
        arbitrary_types_allowed = True

class ActionResponse(BaseModel):
    """Результат выполнения действия."""
    status: str = "ok"
    summary: str = ""
    error_code: str | None = None
    retryable: bool = False
    cooldown_sec: int = 300
    data: dict[str, Any] = Field(default_factory=dict)

class BaseAction(Generic[T], abc.ABC):
    """Базовый класс для всех действий NPC."""
    
    name: str
    description: str
    params_model: type[T]

    @abc.abstractmethod
    async def execute(self, ctx: ActionContext, params: T) -> ActionResponse:
        """Основная логика действия."""
        pass

    def get_schema(self) -> dict[str, Any]:
        """Возвращает JSON-схему параметров для LLM."""
        return self.params_model.model_json_schema()

class ActionRegistry:
    """Реестр всех доступных действий NPC."""
    
    _actions: dict[str, BaseAction] = {}

    @classmethod
    def register(cls, action: BaseAction):
        cls._actions[action.name] = action

    @classmethod
    def get(cls, name: str) -> BaseAction | None:
        return cls._actions.get(name)

    @classmethod
    def list_actions(cls) -> list[BaseAction]:
        return list(cls._actions.values())
