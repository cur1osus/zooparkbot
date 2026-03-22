"""
NPC Game Knowledge Base - Encyclopedia for better decision-making

This module provides strategic game knowledge to help NPCs make informed decisions.
"""

# =============================================================================
# GAME ECONOMY KNOWLEDGE
# =============================================================================

GAME_KNOWLEDGE = {
    # --------------------------------------------------------------------------
    # CURRENCY & EXCHANGE
    # --------------------------------------------------------------------------
    "economy": {
        "currencies": {
            "rub": {
                "name": "Рубли",
                "emoji": "₽",
                "primary_use": "Основная валюта для покупки животных",
                "income_source": "Пассивный доход от животных",
            },
            "usd": {
                "name": "Доллары",
                "emoji": "$",
                "primary_use": "Покупка вольеров, предметов, редких животных",
                "exchange_note": "Можно обменять через банк по выгодному курсу",
            },
            "paw_coins": {
                "name": "Лапки",
                "emoji": "🐾",
                "primary_use": "Создание предметов (альтернатива USD)",
                "rarity": "Редкая валюта",
            },
        },
        "bank_strategy": {
            "best_time": "Обменивать когда курс RUB/USD низкий (15-25)",
            "worst_time": "Избегать обмена при высоком курсе (80+)",
            "fee": "1% комиссия банка",
            "tip": "Копить RUB при высоком курсе, покупать USD при низком",
        },
    },

    # --------------------------------------------------------------------------
    # ANIMALS & RARITY
    # --------------------------------------------------------------------------
    "animals": {
        "rarity_tiers": {
            "_rare": {
                "name": "Редкий",
                "emoji": "🟢",
                "tier": 2,
                "cost_range": "450-500 USD base",
                "income_multiplier": "1.0x",
                "strategy": "Хороши для старта, быстрый ROI",
            },
            "_epic": {
                "name": "Эпический",
                "emoji": "🟣",
                "tier": 3,
                "cost_range": "870-1000 USD base",
                "income_multiplier": "1.5-2.0x",
                "strategy": "Баланс цены и дохода",
            },
            "_mythical": {
                "name": "Мифический",
                "emoji": "🔴",
                "tier": 4,
                "cost_range": "1400-2000 USD base",
                "income_multiplier": "2.5-3.5x",
                "strategy": "Высокий доход, долгий payback",
            },
            "_leg": {
                "name": "Легендарный",
                "emoji": "🟡",
                "tier": 5,
                "cost_range": "1850-3000+ USD base",
                "income_multiplier": "4.0-5.0x",
                "strategy": "Максимальный доход, для лейт-гейма",
            },
        },
        "animal_mechanics": {
            "income_type": "Пассивный доход в RUB/мин",
            "stacking": "Доходы от животных суммируются",
            "unity_bonus": "Топ животное в клане даёт +бонус к доходу",
            "item_bonus": "Предметы могут усиливать доход конкретных животных",
        },
        "buying_strategy": {
            "early_game": "Покупать rare/epic для быстрого роста дохода",
            "mid_game": "Балансировать между epic/mythical",
            "late_game": "Фокус на legendary для максимизации дохода",
            "payback_target": "Целиться в payback < 40 минут",
        },
    },

    # --------------------------------------------------------------------------
    # AVIARIES (CAPACITY)
    # --------------------------------------------------------------------------
    "aviaries": {
        "mechanics": {
            "purpose": "Дают места для животных",
            "price_increase": "30% рост цены после каждой покупки",
            "strategy": "Покупать заранее, пока цена низкая",
        },
        "types": {
            "aviary_1": {
                "name": "Вольер маленький",
                "size": "5 мест",
                "strategy": "Дешёвый, для раннего расширения",
            },
            "aviary_2": {
                "name": "Вольер средний",
                "size": "12 мест",
                "strategy": "Баланс цены и вместимости",
            },
            "aviary_3": {
                "name": "Вольер большой",
                "size": "20 мест",
                "strategy": "Лучшее соотношение цена/место",
            },
        },
        "critical_rules": [
            "0 свободных мест = НЕЛЬЗЯ покупать животных",
            "Цена растёт на 30% после каждой покупки",
            "Покупать вольеры когда afford_quantity > 0",
            "Держать минимум 5-10 мест про запас",
        ],
    },

    # --------------------------------------------------------------------------
    # ITEMS & UPGRADES
    # --------------------------------------------------------------------------
    "items": {
        "creation": {
            "cost_usd": "~300,000 USD (растёт)",
            "cost_paw": "350 лапок",
            "rarity_random": "Случайная редкость предмета",
        },
        "properties": {
            "general_income": "Увеличивает общий доход",
            "animal_income": "Увеличивает доход конкретных животных",
            "animal_sale": "Скидка на покупку животных",
            "aviaries_sale": "Скидка на вольеры",
            "exchange_bank": "Улучшенный курс обмена",
            "bonus_changer": "Перекат бонусов",
            "extra_moves": "Дополнительные ходы",
        },
        "rarity_limits": {
            "common": "1 свойство",
            "rare": "2 свойства",
            "epic": "3 свойства",
            "mythical": "4 свойства",
        },
        "upgrade_mechanics": {
            "cost": "Уровень * USD_TO_UP_ITEM",
            "success_rate": "Падает с уровнем (84% на lvl 3)",
            "fail_result": "Предмет не теряется",
            "max_level": "Зависит от MAX_LVL_ITEM",
        },
        "merge_mechanics": {
            "cost": "USD_TO_MERGE_ITEMS * (props + level_sum)",
            "result": "Объединение свойств двух предметов",
            "limit": "Нельзя превысить лимит свойств rarity",
        },
        "strategy": {
            "active_slots": "Максимум 3 активных предмета",
            "priority": "general_income > animal_income > animal_sale",
            "upgrade_when": "Success rate > 70%",
            "merge_when": "Combined score лучше текущих",
        },
    },

    # --------------------------------------------------------------------------
    # CLANS (UNITY)
    # --------------------------------------------------------------------------
    "clans": {
        "benefits": {
            "level_1": "Базовые функции",
            "level_2": "Скидка на животных (BONUS_DISCOUNT_FOR_ANIMAL_2ND_LVL)",
            "level_3": "Улучшенная скидка + бонусы (BONUS_DISCOUNT_FOR_ANIMAL_3RD_LVL)",
        },
        "requirements": {
            "upgrade_1_to_2": "Нужно количество участников + доход",
            "upgrade_2_to_3": "Больше участников + животные у каждого",
        },
        "projects": {
            "type": "Центр выкупа",
            "deadline": "3 дня",
            "reward": "Сундуки (common/rare/epic)",
            "mvp_bonus": "+1 epic сундук для лучшего контрибьютора",
            "strategy": "Конtribute когда afford, приоритет на MVP",
        },
        "social_strategy": {
            "join_when": "Доход < 500 RUB/min или нет клана",
            "create_when": "Доход > 180 RUB/min + есть USD на создание",
            "recruit_when": "Нужны участники для upgrade",
            "kick_when": "Участник не активен > 7 дней",
        },
    },

    # --------------------------------------------------------------------------
    # BONUSES & DAILY
    # --------------------------------------------------------------------------
    "bonuses": {
        "daily_bonus": {
            "types": ["rub", "usd", "aviary", "animal", "paw_coins"],
            "reroll": "Можно перекатить с предметом bonus_changer",
            "strategy": "Забирать каждый день, reroll если плохой бонус",
        },
        "referral": {
            "reward": "Бонус за приглашённых игроков",
            "leaderboard": "Топ по рефералам даёт награды",
        },
    },

    # --------------------------------------------------------------------------
    # LEADERBOARD & COMPETITION
    # --------------------------------------------------------------------------
    "leaderboard": {
        "categories": [
            "top_income",
            "top_money",
            "top_animals",
            "top_referrals",
        ],
        "rewards": "Награды за топ позиции",
        "pressure": "Конкуренты в топе создают давление",
        "strategy": "Фокус на 1-2 категории для эффективности",
    },

    # --------------------------------------------------------------------------
    # CHAT GAMES & TRANSFERS
    # --------------------------------------------------------------------------
    "chat_features": {
        "games": {
            "types": ["🎯 dart", "🎳 bowling", "🎲 dice", "⚽️ football", "🏀 basketball"],
            "max_players": 80,
            "strategy": "Join когда free slots > 0 и award стоит участия",
        },
        "transfers": {
            "type": "Переводы валюты в чат",
            "claim": "Можно забрать часть перевода",
            "strategy": "Claim бесплатные переводы приоритетно",
        },
    },

    # --------------------------------------------------------------------------
    # STRATEGIC PRIORITIES BY GAME PHASE
    # --------------------------------------------------------------------------
    "phase_strategy": {
        "early_game": {
            "income": "< 500 RUB/min",
            "animals": "< 50",
            "priorities": [
                "Купить первые вольеры (5-10 мест)",
                "Накопить 300-500 USD ликвидности",
                "Покупать rare/epic животных с быстрым ROI",
                "Забирать daily bonus",
                "Вступить в клан или создать свой",
            ],
        },
        "mid_game": {
            "income": "500-2000 RUB/min",
            "animals": "50-200",
            "priorities": [
                "Расширять вольеры (держать 10-20 мест запас)",
                "Покупать epic/mythical животных",
                "Создавать/улучшать предметы",
                "Участвовать в clan projects",
                "Копить на legendary животных",
            ],
        },
        "late_game": {
            "income": "> 2000 RUB/min",
            "animals": "> 200",
            "priorities": [
                "Максимизировать доход (legendary животные)",
                "Оптимизировать предметы (merge/upgrade)",
                "Улучшать клан до level 3",
                "Конкурировать в leaderboard",
                "Помогать клану через transfers/projects",
            ],
        },
    },

    # --------------------------------------------------------------------------
    # KEY THRESHOLDS & TARGETS
    # --------------------------------------------------------------------------
    "thresholds": {
        "liquidity": {
            "minimum": "250 USD (emergency fund)",
            "comfortable": "500-1000 USD",
            "strategic": "2000+ USD (для крупных покупок)",
        },
        "seats": {
            "critical": "0 мест (блокирует покупки)",
            "warning": "< 5 мест",
            "comfortable": "10-20 мест",
            "target": "20% запас от текущих животных",
        },
        "income_milestones": [
            100, 300, 500, 1000, 2000, 5000, 10000
        ],
        "unity_creation": {
            "minimum_income": "180 RUB/min",
            "recommended_usd": "1000 USD на создание",
        },
    },

    # --------------------------------------------------------------------------
    # COMMON MISTAKES TO AVOID
    # --------------------------------------------------------------------------
    "mistakes": {
        "critical": [
            "Покупать животных когда 0 мест (нельзя)",
            "Тратить все USD без ликвидности",
            "Игнорировать рост цен на вольеры (30%)",
            "Не забирать daily bonus",
        ],
        "suboptimal": [
            "Покупать животных с payback > 60 минут",
            "Upgrade предметов с success rate < 50%",
            "Держать > 3 активных предметов (лимит)",
            "Игнорировать clan projects (бесплатные награды)",
            "Не обменивать RUB при выгодном курсе",
        ],
    },
}


# =============================================================================
# HELPER FUNCTIONS FOR NPC
# =============================================================================

def get_knowledge_for_context(category: str, subcategory: str = None) -> dict:
    """Get specific knowledge for NPC context."""
    if subcategory:
        return GAME_KNOWLEDGE.get(category, {}).get(subcategory, {})
    return GAME_KNOWLEDGE.get(category, {})


def get_phase_strategy(income: int, animals: int) -> dict:
    """Determine current game phase and return strategy."""
    if income < 500 and animals < 50:
        return GAME_KNOWLEDGE["phase_strategy"]["early_game"]
    elif income < 2000 and animals < 200:
        return GAME_KNOWLEDGE["phase_strategy"]["mid_game"]
    else:
        return GAME_KNOWLEDGE["phase_strategy"]["late_game"]


def check_threshold_warning(metric: str, value: int) -> str | None:
    """Return warning message if value is near critical threshold."""
    thresholds = GAME_KNOWLEDGE["thresholds"]
    
    if metric == "seats":
        if value <= 0:
            return "CRITICAL: No seats - cannot buy animals!"
        elif value < 5:
            return "WARNING: Low seats - buy aviary soon"
    
    if metric == "usd":
        if value < 250:
            return "WARNING: Low liquidity - consider hoarding USD"
    
    return None


def get_animal_rarity_info(rarity: str) -> dict:
    """Get info about specific animal rarity tier."""
    return GAME_KNOWLEDGE["animals"]["rarity_tiers"].get(rarity, {})


def get_item_property_weight(prop_name: str) -> float:
    """Get strategic weight for item property (higher = better)."""
    weights = {
        "general_income": 5.0,
        "animal_income": 4.0,
        "animal_sale": 3.5,
        "aviaries_sale": 3.0,
        "exchange_bank": 2.5,
        "bonus_changer": 1.2,
        "extra_moves": 0.8,
    }
    # Extract base property name (e.g., "animal1_rare:animal_income" -> "animal_income")
    base_prop = prop_name.split(":")[-1] if ":" in prop_name else prop_name
    return weights.get(base_prop, 1.0)
