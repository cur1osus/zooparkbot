from __future__ import annotations

import contextlib
import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, TYPE_CHECKING

from pydantic import BaseModel, Field

from aiogram.utils.deep_linking import create_start_link
from config import CHAT_ID
from db import Animal, Game, Gamer, Item, RequestToUnity, TransferMoney, Unity, User
from db.structured_state import (
    add_unity_member,
    get_user_aviaries_map,
    pop_next_unity_owner,
    remove_unity_member,
)
from game_variables import games
from init_bot import bot
from init_db_redis import redis
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tools.animals import add_animal, get_price_animal
from tools.aviaries import add_aviary, get_price_aviaries, get_remain_seats
from tools.bank import exchange, get_rate
from tools.bonus import apply_bonus, get_bonus

from tools.items import (
    CREATE_ITEM_PAW_PRICE,
    add_item_to_db,
    able_to_enhance,
    create_item,
    gen_price_to_create_item,
    get_value_prop_from_iai,
    merge_items,
    random_up_property_item,
    synchronize_info_about_items,
    update_prop_iai,
)
from tools.random_merchant import gen_price
from tools.unity import get_unity_idpk

import tools
from tools import add_to_currency, add_user_to_used, gen_key, in_used
from tools.value import get_value
from tools.unity_projects import (
    contribute_to_project,
    get_user_chests,
    open_user_chests,
)
from text_utils import fit_db_field, normalize_choice, preview_text

from bot.keyboards import (
    ik_get_money,
    ik_get_money_one_piece,
    ik_npc_unity_invitation,
    ik_start_created_game,
)
from tools.message import get_id_for_edit_message

from ..state_builder import (
    can_upgrade_unity,
    ensure_random_merchant_for_user,
    find_cheapest_affordable_animal,
    generate_npc_unity_name_via_llm,
    get_active_user_items,
    get_user_item,
    npc_unity_invite_key,
    optimize_items_for_user,
    safe_int,
    sanitize_unity_name,
)
from ..settings import settings
from .base import BaseAction, ActionContext, ActionResponse, ActionRegistry

if TYPE_CHECKING:
    from ..client import NpcDecisionClient

class WaitParams(BaseModel):
    pass

class WaitAction(BaseAction[WaitParams]):
    name = "wait"
    description = "Wait for some time without doing any action."
    params_model = WaitParams

    async def execute(self, ctx: ActionContext, params: WaitParams) -> ActionResponse:
        return ActionResponse(status="ok", summary="wait")

class ChangeOwnMoodParams(BaseModel):
    mood: str = Field(default="neutral", max_length=32)

class ChangeOwnMoodAction(BaseAction[ChangeOwnMoodParams]):
    name = "change_own_mood"
    description = "Change NPC's internal mood state."
    params_model = ChangeOwnMoodParams

    async def execute(self, ctx: ActionContext, params: ChangeOwnMoodParams) -> ActionResponse:
        from ..memory import (
            ensure_npc_profile_memory,
            _json_loads,
            _rehydrate_profile_payload,
        )

        profile_row = await ensure_npc_profile_memory(ctx.session, ctx.user)
        profile = _rehydrate_profile_payload(
            user=ctx.user, payload=_json_loads(profile_row.payload)
        )
        mood = fit_db_field(params.mood, max_len=32, default="neutral")
        profile["current_mood"] = mood
        profile_row.payload = json.dumps(profile)
        await ctx.session.flush()
        return ActionResponse(status="ok", summary=f"mood changed to {mood}")

class SetTacticalFocusParams(BaseModel):
    focus: str = Field(default="economy", max_length=32)

class SetTacticalFocusAction(BaseAction[SetTacticalFocusParams]):
    name = "set_tactical_focus"
    description = "Add a new tactical focus to NPC's profile."
    params_model = SetTacticalFocusParams

    async def execute(self, ctx: ActionContext, params: SetTacticalFocusParams) -> ActionResponse:
        from ..memory import (
            ensure_npc_profile_memory,
            _json_loads,
            _rehydrate_profile_payload,
        )

        profile_row = await ensure_npc_profile_memory(ctx.session, ctx.user)
        profile = _rehydrate_profile_payload(
            user=ctx.user, payload=_json_loads(profile_row.payload)
        )
        focus = fit_db_field(params.focus, max_len=32, default="economy")
        tactics = profile.get("active_tactics", [])
        if isinstance(tactics, list):
            if focus not in tactics:
                tactics.append(focus)
            profile["active_tactics"] = tactics[-3:]
        profile_row.payload = json.dumps(profile)
        await ctx.session.flush()
        return ActionResponse(status="ok", summary=f"tactical focus included {focus}")

class SendNpcSignalParams(BaseModel):
    target_idpk: int
    signal_type: Literal["request_funds", "propose_alliance", "taunt", "info"] = Field(default="info")
    message: str = Field(default="", max_length=100)

class SendNpcSignalAction(BaseAction[SendNpcSignalParams]):
    name = "send_npc_signal"
    description = "Send a signal/message to another NPC."
    params_model = SendNpcSignalParams

    async def execute(self, ctx: ActionContext, params: SendNpcSignalParams) -> ActionResponse:
        if not params.target_idpk or params.target_idpk == ctx.user.idpk:
            return ActionResponse(status="error", summary="invalid_target_idpk")

        target_user = await ctx.session.get(User, params.target_idpk)
        if not target_user:
            return ActionResponse(status="error", summary="target_not_found")

        # only npc can receive ping this way for now
        if not (target_user.id_user < 0 or target_user.username.startswith("npc_")):
            return ActionResponse(status="error", summary="target_not_npc")

        signal_type = normalize_choice(
            params.signal_type,
            allowed={"request_funds", "propose_alliance", "taunt", "info"},
            default="info",
        )
        message = preview_text(params.message, max_chars=100, placeholder="...")

        from ..memory import NpcMemory, FACT_KIND

        signal_fact = NpcMemory(
            idpk_user=params.target_idpk,
            kind=FACT_KIND,
            topic=f"incoming_signal:{ctx.user.idpk}",
            payload=json.dumps(
                {
                    "fact": f"Signal '{signal_type}' from {ctx.user.nickname} (id:{ctx.user.idpk}): {message}",
                    "confidence": 1000,
                    "source": "npc_link",
                }
            ),
        )
        ctx.session.add(signal_fact)
        await ctx.session.flush()

        return ActionResponse(status="ok", summary=f"signal sent to {target_user.nickname}")

class ClaimDailyBonusParams(BaseModel):
    rerolls: int = Field(default=0, ge=0)

class ClaimDailyBonusAction(BaseAction[ClaimDailyBonusParams]):
    name = "claim_daily_bonus"
    description = "Claim the daily bonus, optionally rerolling if item permits."
    params_model = ClaimDailyBonusParams

    async def execute(self, ctx: ActionContext, params: ClaimDailyBonusParams) -> ActionResponse:
        if not ctx.user.bonus:
            return ActionResponse(status="skipped", summary="no_bonus")
        
        max_rerolls = int(
            get_value_prop_from_iai(
                info_about_items=ctx.user.info_about_items,
                name_prop="bonus_changer",
            )
            or 0
        )
        rerolls = min(params.rerolls, max_rerolls)
        data_bonus = await get_bonus(session=ctx.session, user=ctx.user)
        while rerolls > 0:
            data_bonus = await get_bonus(session=ctx.session, user=ctx.user)
            rerolls -= 1
        ctx.user.bonus -= 1
        await apply_bonus(session=ctx.session, user=ctx.user, data_bonus=data_bonus)
        return ActionResponse(status="ok", summary=f"bonus:{data_bonus.bonus_type}")

class BuyAviaryParams(BaseModel):
    code_name_aviary: str
    quantity: int = Field(default=1, ge=1)

class BuyAviaryAction(BaseAction[BuyAviaryParams]):
    name = "buy_aviary"
    description = "Purchase one or more aviaries of a specific type."
    params_model = BuyAviaryParams

    async def execute(self, ctx: ActionContext, params: BuyAviaryParams) -> ActionResponse:
        code_name_aviary = params.code_name_aviary.strip()
        quantity = params.quantity
        if not code_name_aviary:
            return ActionResponse(status="skipped", summary="aviary_missing")

        aviaries_state = await get_user_aviaries_map(session=ctx.session, user=ctx.user)
        aviary_price = await get_price_aviaries(
            session=ctx.session,
            aviaries=aviaries_state,
            code_name_aviary=code_name_aviary,
            info_about_items=ctx.user.info_about_items,
        )
        finite_price = aviary_price * quantity
        if int(ctx.user.usd) < finite_price:
            return ActionResponse(status="skipped", summary="not_enough_usd", error_code="not_enough_usd")

        ctx.user.usd -= finite_price
        ctx.user.amount_expenses_usd += finite_price
        await add_aviary(
            session=ctx.session,
            self=ctx.user,
            code_name_aviary=code_name_aviary,
            quantity=quantity,
        )
        return ActionResponse(
            status="ok",
            summary=f"buy_aviary:{code_name_aviary}x{quantity}",
        )

class BuyRarityAnimalParams(BaseModel):
    animal: str
    rarity: Literal["_rare", "_epic", "_mythical", "_leg"]
    quantity: int = Field(default=1, ge=1)

class BuyRarityAnimalAction(BaseAction[BuyRarityAnimalParams]):
    name = "buy_rarity_animal"
    description = "Purchase animals of a specific rarity."
    params_model = BuyRarityAnimalParams

    async def execute(self, ctx: ActionContext, params: BuyRarityAnimalParams) -> ActionResponse:
        animal = params.animal.strip()
        rarity = params.rarity
        quantity = params.quantity
        if not animal:
            return ActionResponse(status="skipped", summary="bad_animal_params")

        remain_seats = await get_remain_seats(session=ctx.session, user=ctx.user)
        if remain_seats <= 0:
            return ActionResponse(status="skipped", summary="no_seat_capacity", error_code="no_seat_capacity")
        quantity = min(quantity, int(remain_seats))
        if remain_seats < quantity:
            return ActionResponse(status="skipped", summary="not_enough_seats", error_code="not_enough_seats")

        unity_idpk = int(get_unity_idpk(ctx.user.current_unity) or 0) or None
        code_name = f"{animal}{rarity}"
        animal_price = await get_price_animal(
            session=ctx.session,
            animal_code_name=code_name,
            unity_idpk=unity_idpk,
            info_about_items=ctx.user.info_about_items,
        )
        finite_price = animal_price * quantity
        if int(ctx.user.usd) < finite_price:
            return ActionResponse(status="skipped", summary="not_enough_usd", error_code="not_enough_usd")

        ctx.user.usd -= finite_price
        ctx.user.amount_expenses_usd += finite_price
        await add_animal(
            self=ctx.user,
            code_name_animal=code_name,
            quantity=quantity,
            session=ctx.session,
        )
        return ActionResponse(status="ok", summary=f"buy_animal:{code_name}x{quantity}")

class CreateItemParams(BaseModel):
    pass

class CreateItemAction(BaseAction[CreateItemParams]):
    name = "create_item"
    description = "Create a new random item for the user."
    params_model = CreateItemParams

    async def execute(self, ctx: ActionContext, params: CreateItemParams) -> ActionResponse:
        create_price = await gen_price_to_create_item(session=ctx.session, id_user=ctx.user.id_user)
        if int(ctx.user.usd) >= create_price:
            ctx.user.usd -= create_price
            ctx.user.amount_expenses_usd += create_price
        elif int(ctx.user.paw_coins) >= CREATE_ITEM_PAW_PRICE:
            ctx.user.paw_coins -= CREATE_ITEM_PAW_PRICE
            ctx.user.amount_expenses_paw_coins += CREATE_ITEM_PAW_PRICE
        else:
            return ActionResponse(status="skipped", summary="not_enough_create_currency")

        item_info, item_props = await create_item(session=ctx.session)
        await add_item_to_db(
            session=ctx.session,
            item_info=item_info,
            item_props=item_props,
            id_user=ctx.user.id_user,
        )
        await optimize_items_for_user(session=ctx.session, user=ctx.user)
        return ActionResponse(status="ok", summary=f"create_item:{item_info['key']}")

class BuyMerchantDiscountOfferParams(BaseModel):
    pass

class BuyMerchantDiscountOfferAction(BaseAction[BuyMerchantDiscountOfferParams]):
    name = "buy_merchant_discount_offer"
    description = "Buy the current discounted offer from the random merchant."
    params_model = BuyMerchantDiscountOfferParams

    async def execute(self, ctx: ActionContext, params: BuyMerchantDiscountOfferParams) -> ActionResponse:
        merchant = await ensure_random_merchant_for_user(session=ctx.session, user=ctx.user)
        if merchant.first_offer_bought:
            return ActionResponse(status="skipped", summary="merchant_offer_used")
        remain_seats = await get_remain_seats(session=ctx.session, user=ctx.user)
        if remain_seats < merchant.quantity_animals:
            return ActionResponse(status="skipped", summary="not_enough_seats", error_code="not_enough_seats")
        if int(ctx.user.usd) < merchant.price_with_discount:
            return ActionResponse(status="skipped", summary="not_enough_usd", error_code="not_enough_usd")

        ctx.user.usd -= merchant.price_with_discount
        ctx.user.amount_expenses_usd += merchant.price_with_discount
        await add_animal(
            self=ctx.user,
            code_name_animal=merchant.code_name_animal,
            quantity=merchant.quantity_animals,
            session=ctx.session,
        )
        merchant.first_offer_bought = True
        return ActionResponse(
            status="ok",
            summary=f"merchant_discount:{merchant.code_name_animal}x{merchant.quantity_animals}",
        )

class BuyMerchantTargetedOfferParams(BaseModel):
    animal: str
    quantity: int = Field(default=1, ge=1)

class BuyMerchantTargetedOfferAction(BaseAction[BuyMerchantTargetedOfferParams]):
    name = "buy_merchant_targeted_offer"
    description = "Buy a targeted animal offer from the merchant."
    params_model = BuyMerchantTargetedOfferParams

    async def execute(self, ctx: ActionContext, params: BuyMerchantTargetedOfferParams) -> ActionResponse:
        from tools.animals import get_animal_with_random_rarity

        animal = params.animal.strip()
        quantity = params.quantity
        if not animal:
            return ActionResponse(status="skipped", summary="animal_missing")
        remain_seats = await get_remain_seats(session=ctx.session, user=ctx.user)
        if remain_seats < quantity:
            return ActionResponse(status="skipped", summary="not_enough_seats", error_code="not_enough_seats")
        animal_price = await ctx.session.scalar(
            select(Animal.price).where(Animal.code_name == f"{animal}-")
        )
        if not animal_price:
            return ActionResponse(status="skipped", summary="animal_not_found")
        finite_price = int(animal_price) * quantity
        if int(ctx.user.usd) < finite_price:
            return ActionResponse(status="skipped", summary="not_enough_usd", error_code="not_enough_usd")

        ctx.user.usd -= finite_price
        ctx.user.amount_expenses_usd += finite_price
        rewards = []
        while quantity > 0:
            animal_obj = await get_animal_with_random_rarity(session=ctx.session, animal=animal)
            part_animals = min(quantity, max(1, quantity // 2))
            quantity -= part_animals
            await add_animal(
                self=ctx.user,
                code_name_animal=animal_obj.code_name,
                quantity=part_animals,
                session=ctx.session,
            )
            rewards.append(f"{animal_obj.code_name}x{part_animals}")
        return ActionResponse(status="ok", summary=f"merchant_targeted:{','.join(rewards)}")

class InvestForIncomeParams(BaseModel):
    pass

class InvestForIncomeAction(BaseAction[InvestForIncomeParams]):
    name = "invest_for_income"
    description = "Automatically invest in animals or aviaries to increase income."
    params_model = InvestForIncomeParams

    async def execute(self, ctx: ActionContext, params: InvestForIncomeParams) -> ActionResponse:
        signal = ctx.observation["strategy_signals"]["summary"]
        if signal["need_seats"] and signal["best_aviary_option"]:
            best_aviary = signal["best_aviary_option"]
            aviary_size = max(1, int(best_aviary.get("size", 1) or 1))
            affordable_quantity = max(
                1, int(best_aviary.get("affordable_quantity", 1) or 1)
            )
            target_new_seats = max(aviary_size, 6)
            quantity = max(1, (target_new_seats + aviary_size - 1) // aviary_size)
            quantity = min(quantity, affordable_quantity)

            action = ActionRegistry.get("buy_aviary")
            if action:
                return await action.execute(
                    ctx, BuyAviaryParams(code_name_aviary=best_aviary["code_name"], quantity=quantity)
                )

        best_income_option = signal.get("best_income_option")
        if best_income_option:
            quantity = max(
                1,
                min(
                    int(best_income_option.get("affordable_quantity", 1) or 1),
                    int(ctx.observation.get("zoo", {}).get("remain_seats", 1) or 1),
                ),
            )
            action = ActionRegistry.get("buy_rarity_animal")
            if action:
                return await action.execute(
                    ctx, BuyRarityAnimalParams(
                        animal=best_income_option["animal"], 
                        rarity=best_income_option["rarity"], 
                        quantity=quantity
                    )
                )

        if (
            int(ctx.user.usd) >= ctx.observation["items"]["create_price_usd"]
            or int(ctx.user.paw_coins) >= CREATE_ITEM_PAW_PRICE
        ):
            action = ActionRegistry.get("create_item")
            if action:
                return await action.execute(ctx, CreateItemParams())

        return ActionResponse(status="skipped", summary="no_income_investment_found")

class InvestForTopAnimalsParams(BaseModel):
    pass

class InvestForTopAnimalsAction(BaseAction[InvestForTopAnimalsParams]):
    name = "invest_for_top_animals"
    description = "Invest in high-value animals from the merchant or targeted offers."
    params_model = InvestForTopAnimalsParams

    async def execute(self, ctx: ActionContext, params: InvestForTopAnimalsParams) -> ActionResponse:
        if ctx.observation["merchant"]["first_offer_bought"] is False:
            merchant_quantity = int(ctx.observation["merchant"]["quantity_animals"])
            if merchant_quantity > 1:
                action = ActionRegistry.get("buy_merchant_discount_offer")
                if action:
                    res = await action.execute(ctx, BuyMerchantDiscountOfferParams())
                    if res.status == "ok":
                        return res

        if ctx.observation["zoo"]["remain_seats"] <= 0:
            cheapest_aviary = ctx.observation["strategy_signals"]["summary"].get("best_aviary_option")
            if cheapest_aviary:
                action = ActionRegistry.get("buy_aviary")
                if action:
                    return await action.execute(
                        ctx, BuyAviaryParams(code_name_aviary=cheapest_aviary["code_name"], quantity=1)
                    )

        cheapest_target = find_cheapest_affordable_animal(observation=ctx.observation)
        if cheapest_target:
            action = ActionRegistry.get("buy_merchant_targeted_offer")
            if action:
                return await action.execute(
                    ctx, BuyMerchantTargetedOfferParams(
                        animal=cheapest_target,
                        quantity=max(1, ctx.observation["zoo"]["remain_seats"])
                    )
                )
        return ActionResponse(status="skipped", summary="no_top_animals_investment_found")

class ExchangeBankParams(BaseModel):
    mode: Literal["all", "amount"] = Field(default="all")
    amount: int | None = Field(default=None, description="Amount of RUB to exchange if mode is 'amount'.")

class ExchangeBankAction(BaseAction[ExchangeBankParams]):
    name = "exchange_bank"
    description = "Exchange RUB for USD at the current bank rate."
    params_model = ExchangeBankParams

    async def execute(self, ctx: ActionContext, params: ExchangeBankParams) -> ActionResponse:
        rate = await get_rate(session=ctx.session, user=ctx.user)
        if int(ctx.user.rub) < rate:
            return ActionResponse(status="skipped", summary="not_enough_rub", error_code="bank_no_funds")

        if params.mode == "amount" and params.amount:
            if params.amount < rate:
                return ActionResponse(status="skipped", summary="amount_too_small", error_code="bank_min_amount")
            amount = min(params.amount, int(ctx.user.rub))
            you_change, bank_fee, you_got = await exchange(
                session=ctx.session, user=ctx.user, amount=amount, rate=rate, all=False
            )
        else:
            you_change, bank_fee, you_got = await exchange(
                session=ctx.session, user=ctx.user, amount=int(ctx.user.rub), rate=rate, all=True
            )

        ctx.user.usd += you_got
        return ActionResponse(
            status="ok", 
            summary=f"exchange:{you_change}->{you_got}",
            data={"bank_fee": bank_fee}
        )

class BuyMerchantRandomOfferParams(BaseModel):
    pass

class BuyMerchantRandomOfferAction(BaseAction[BuyMerchantRandomOfferParams]):
    name = "buy_merchant_random_offer"
    description = "Buy a random animal offer from the merchant."
    params_model = BuyMerchantRandomOfferParams

    async def execute(self, ctx: ActionContext, params: BuyMerchantRandomOfferParams) -> ActionResponse:
        from tools.animals import gen_quantity_animals, get_random_animal

        merchant = await ensure_random_merchant_for_user(session=ctx.session, user=ctx.user)
        max_quantity_animals = await get_value(
            session=ctx.session, value_name="MAX_QUANTITY_ANIMALS"
        )
        remain_seats = await get_remain_seats(session=ctx.session, user=ctx.user)
        if remain_seats < max_quantity_animals:
            return ActionResponse(status="skipped", summary="not_enough_seats", error_code="not_enough_seats")
        if int(ctx.user.usd) < merchant.price:
            return ActionResponse(status="skipped", summary="not_enough_usd", error_code="not_enough_usd")

        ctx.user.usd -= merchant.price
        ctx.user.amount_expenses_usd += merchant.price
        quantity_animals = await gen_quantity_animals(session=ctx.session, user=ctx.user)
        rewards = []
        while quantity_animals > 0:
            animal_obj = await get_random_animal(session=ctx.session, user=ctx.user)
            part_animals = min(quantity_animals, max(1, quantity_animals // 2))
            quantity_animals -= part_animals
            await add_animal(
                self=ctx.user,
                code_name_animal=animal_obj.code_name,
                quantity=part_animals,
                session=ctx.session,
            )
            rewards.append(f"{animal_obj.code_name}x{part_animals}")
        merchant.price = await gen_price(session=ctx.session, user=ctx.user)
        return ActionResponse(status="ok", summary=f"merchant_random:{','.join(rewards)}")

class ActivateItemParams(BaseModel):
    id_item: str

class ActivateItemAction(BaseAction[ActivateItemParams]):
    name = "activate_item"
    description = "Activate a specific item in the inventory."
    params_model = ActivateItemParams

    async def execute(self, ctx: ActionContext, params: ActivateItemParams) -> ActionResponse:
        id_item = params.id_item.strip()
        item = await get_user_item(session=ctx.session, user=ctx.user, id_item=id_item)
        if not item:
            return ActionResponse(status="skipped", summary="item_not_found")
        if item.is_active:
            return ActionResponse(status="skipped", summary="item_already_active")

        active_items = await get_active_user_items(session=ctx.session, user=ctx.user)
        if len(active_items) >= 3:
            return ActionResponse(status="skipped", summary="max_active_items")

        item.is_active = True
        active_items.append(item)
        ctx.user.info_about_items = await synchronize_info_about_items(items=active_items)
        await tools.sync_user_income(session=ctx.session, user=ctx.user)
        return ActionResponse(status="ok", summary=f"activate_item:{id_item}")

class DeactivateItemParams(BaseModel):
    id_item: str

class DeactivateItemAction(BaseAction[DeactivateItemParams]):
    name = "deactivate_item"
    description = "Deactivate a currently active item."
    params_model = DeactivateItemParams

    async def execute(self, ctx: ActionContext, params: DeactivateItemParams) -> ActionResponse:
        id_item = params.id_item.strip()
        item = await get_user_item(session=ctx.session, user=ctx.user, id_item=id_item)
        if not item:
            return ActionResponse(status="skipped", summary="item_not_found")
        if not item.is_active:
            return ActionResponse(status="skipped", summary="item_not_active")

        item.is_active = False
        active_items = await get_active_user_items(session=ctx.session, user=ctx.user)
        ctx.user.info_about_items = await synchronize_info_about_items(items=active_items)
        await tools.sync_user_income(session=ctx.session, user=ctx.user)
        return ActionResponse(status="ok", summary=f"deactivate_item:{id_item}")

class SellItemParams(BaseModel):
    id_item: str

class SellItemAction(BaseAction[SellItemParams]):
    name = "sell_item"
    description = "Sell an item from the inventory for USD."
    params_model = SellItemParams

    async def execute(self, ctx: ActionContext, params: SellItemParams) -> ActionResponse:
        id_item = params.id_item.strip()
        item = await get_user_item(session=ctx.session, user=ctx.user, id_item=id_item)
        if not item:
            return ActionResponse(status="skipped", summary="item_not_found")

        usd_to_create_item = await get_value(
            session=ctx.session, value_name="USD_TO_CREATE_ITEM"
        )
        percent_markdown_item = await get_value(
            session=ctx.session,
            value_name="PERCENT_MARKDOWN_ITEM",
        )
        sell_price = int(int(usd_to_create_item) * (int(percent_markdown_item) / 100))
        item.id_user = 0
        item.is_active = False
        ctx.user.usd += sell_price
        active_items = await get_active_user_items(session=ctx.session, user=ctx.user)
        ctx.user.info_about_items = await synchronize_info_about_items(items=active_items)
        await tools.sync_user_income(session=ctx.session, user=ctx.user)
        return ActionResponse(status="ok", summary=f"sell_item:{id_item}:{sell_price}")

class OptimizeItemsParams(BaseModel):
    pass

class OptimizeItemsAction(BaseAction[OptimizeItemsParams]):
    name = "optimize_items"
    description = "Automatically activate the best items in the inventory."
    params_model = OptimizeItemsParams

    async def execute(self, ctx: ActionContext, params: OptimizeItemsParams) -> ActionResponse:
        changed = await optimize_items_for_user(session=ctx.session, user=ctx.user)
        return ActionResponse(status="ok", summary=f"optimize_items:{changed}")

class UpgradeItemParams(BaseModel):
    id_item: str

class UpgradeItemAction(BaseAction[UpgradeItemParams]):
    name = "upgrade_item"
    description = "Upgrade the level of a specific item."
    params_model = UpgradeItemParams

    async def execute(self, ctx: ActionContext, params: UpgradeItemParams) -> ActionResponse:
        id_item = params.id_item.strip()
        if not id_item:
            return ActionResponse(status="skipped", summary="item_missing")
        item = await ctx.session.scalar(
            select(Item).where(Item.id_item == id_item, Item.id_user == ctx.user.id_user)
        )
        if not item:
            return ActionResponse(status="skipped", summary="item_not_found")
        max_lvl_item = await get_value(session=ctx.session, value_name="MAX_LVL_ITEM")
        if item.lvl >= max_lvl_item:
            return ActionResponse(status="skipped", summary="item_max_level")
        usd_to_up_item = await get_value(session=ctx.session, value_name="USD_TO_UP_ITEM")
        cost = int(usd_to_up_item) * (int(item.lvl) + 1)
        if int(ctx.user.usd) < cost:
            return ActionResponse(status="skipped", summary="not_enough_usd", error_code="not_enough_usd")

        ctx.user.usd -= cost
        ctx.user.amount_expenses_usd += cost
        if not await able_to_enhance(session=ctx.session, current_item_lvl=item.lvl):
            return ActionResponse(status="ok", summary=f"upgrade_failed:{id_item}")

        new_item_properties, updated_property, parameter = await random_up_property_item(
            session=ctx.session,
            item_properties=item.properties,
        )
        if item.is_active:
            ctx.user.info_about_items = await update_prop_iai(
                info_about_items=ctx.user.info_about_items,
                prop=updated_property,
                value=parameter,
            )
            await tools.sync_user_income(session=ctx.session, user=ctx.user)
        item.properties = new_item_properties
        item.lvl += 1
        return ActionResponse(
            status="ok",
            summary=f"upgrade_item:{id_item}:{updated_property}+={parameter}",
        )

class MergeItemsParams(BaseModel):
    id_item_1: str
    id_item_2: str

class MergeItemsAction(BaseAction[MergeItemsParams]):
    name = "merge_items"
    description = "Merge two items into a new one."
    params_model = MergeItemsParams

    async def execute(self, ctx: ActionContext, params: MergeItemsParams) -> ActionResponse:
        id_item_1 = params.id_item_1.strip()
        id_item_2 = params.id_item_2.strip()
        if not id_item_1 or not id_item_2 or id_item_1 == id_item_2:
            return ActionResponse(status="skipped", summary="bad_merge_params")

        item_1 = await ctx.session.scalar(
            select(Item).where(Item.id_item == id_item_1, Item.id_user == ctx.user.id_user)
        )
        item_2 = await ctx.session.scalar(
            select(Item).where(Item.id_item == id_item_2, Item.id_user == ctx.user.id_user)
        )
        if not item_1 or not item_2:
            return ActionResponse(status="skipped", summary="merge_items_not_found")

        usd_to_merge_items = await get_value(
            session=ctx.session, value_name="USD_TO_MERGE_ITEMS"
        )
        q_props = len(json.loads(item_1.properties)) + len(json.loads(item_2.properties))
        lvl_sum = max(1, int(item_1.lvl) + int(item_2.lvl))
        cost = int(usd_to_merge_items) * (q_props + lvl_sum)
        if int(ctx.user.usd) < cost:
            return ActionResponse(status="skipped", summary="not_enough_usd", error_code="not_enough_usd")

        ctx.user.usd -= cost
        ctx.user.amount_expenses_usd += cost
        new_item = await merge_items(
            session=ctx.session,
            id_item_1=id_item_1,
            id_item_2=id_item_2,
        )
        new_item.id_user = ctx.user.id_user
        ctx.session.add(new_item)
        await optimize_items_for_user(session=ctx.session, user=ctx.user)
        return ActionResponse(status="ok", summary=f"merge_items:{id_item_1}+{id_item_2}")

class CreateUnityParams(BaseModel):
    name: str | None = Field(default=None)

class CreateUnityAction(BaseAction[CreateUnityParams]):
    name = "create_unity"
    description = "Create a new unity (clan)."
    params_model = CreateUnityParams

    async def execute(self, ctx: ActionContext, params: CreateUnityParams) -> ActionResponse:
        if ctx.user.current_unity:
            return ActionResponse(status="skipped", summary="already_in_unity")
        price_for_create_unity = await get_value(
            session=ctx.session,
            value_name="PRICE_FOR_CREATE_UNITY",
        )
        if int(ctx.user.usd) < price_for_create_unity:
            return ActionResponse(status="skipped", summary="not_enough_usd", error_code="not_enough_usd")

        provided_name = sanitize_unity_name(
            (params.name or "").strip(),
            int(await get_value(session=ctx.session, value_name="NAME_UNITY_LENGTH_MAX")),
        )
        name = provided_name or await generate_npc_unity_name_via_llm(
            session=ctx.session,
            user=ctx.user,
            observation=ctx.observation,
            client=ctx.client,
        )
        unity = Unity(idpk_user=ctx.user.idpk, name=name)
        ctx.user.usd -= price_for_create_unity
        ctx.user.amount_expenses_usd += price_for_create_unity
        ctx.session.add(unity)
        await ctx.session.flush()
        ctx.user.current_unity = f"owner:{unity.idpk}"
        await tools.sync_user_income(session=ctx.session, user=ctx.user)
        return ActionResponse(status="ok", summary=f"create_unity:{unity.name}")

class JoinBestUnityParams(BaseModel):
    owner_idpk: int | None = Field(default=None)

class JoinBestUnityAction(BaseAction[JoinBestUnityParams]):
    name = "join_best_unity"
    description = "Join a unity, optionally specifying the owner's IDPK."
    params_model = JoinBestUnityParams

    async def execute(self, ctx: ActionContext, params: JoinBestUnityParams) -> ActionResponse:
        if ctx.user.current_unity:
            return ActionResponse(status="skipped", summary="already_in_unity")

        owner_idpk = params.owner_idpk
        candidates = ctx.observation["unity"]["candidates"]
        chosen = None
        for candidate in candidates:
            if owner_idpk and candidate["owner_idpk"] == owner_idpk:
                chosen = candidate
                break
        if not chosen and candidates:
            chosen = candidates[0]
        if not chosen:
            return ActionResponse(status="skipped", summary="no_unity_candidates")

        unity = await ctx.session.scalar(
            select(Unity).where(Unity.idpk_user == chosen["owner_idpk"])
        )
        if not unity:
            return ActionResponse(status="skipped", summary="unity_not_found")

        owner = await ctx.session.get(User, chosen["owner_idpk"])
        if owner and owner.id_user < 0:
            await add_unity_member(session=ctx.session, unity=unity, member_idpk=ctx.user.idpk)
            ctx.user.current_unity = f"member:{unity.idpk}"
            await tools.sync_user_income(session=ctx.session, user=ctx.user)
            return ActionResponse(status="ok", summary=f"join_npc_unity:{unity.name}")

        existing_request = await ctx.session.scalar(
            select(RequestToUnity).where(RequestToUnity.idpk_user == ctx.user.idpk)
        )
        if existing_request:
            return ActionResponse(status="skipped", summary="unity_request_exists", error_code="unity_request_exists")

        min_to_end_request = await get_value(
            session=ctx.session, value_name="MIN_TO_END_REQUEST"
        )
        request = RequestToUnity(
            idpk_user=ctx.user.idpk,
            idpk_unity_owner=unity.idpk_user,
            date_request=datetime.now(),
            date_request_end=datetime.now() + timedelta(minutes=int(min_to_end_request)),
        )
        ctx.session.add(request)
        return ActionResponse(status="ok", summary=f"request_unity:{unity.name}")

class RecruitTopPlayerParams(BaseModel):
    idpk_user: int | None = Field(default=None)

class RecruitTopPlayerAction(BaseAction[RecruitTopPlayerParams]):
    name = "recruit_top_player"
    description = "Recruit a top player to your unity."
    params_model = RecruitTopPlayerParams

    async def execute(self, ctx: ActionContext, params: RecruitTopPlayerParams) -> ActionResponse:
        current_unity = ctx.observation["unity"].get("current")
        if not current_unity or not current_unity.get("is_owner"):
            return ActionResponse(status="skipped", summary="not_unity_owner")

        target_idpk = params.idpk_user
        recruit_targets = ctx.observation["unity"].get("recruit_targets", [])
        target = None
        for candidate in recruit_targets:
            if target_idpk and candidate["idpk"] == target_idpk:
                target = candidate
                break
        if not target and recruit_targets:
            target = recruit_targets[0]
        if not target:
            return ActionResponse(status="skipped", summary="no_recruit_targets")

        invited_user = await ctx.session.get(User, target["idpk"])
        unity = await ctx.session.get(Unity, current_unity["idpk"])
        if not invited_user or not unity or invited_user.current_unity:
            return ActionResponse(status="skipped", summary="recruit_target_unavailable", error_code="recruit_target_unavailable")

        invite_key = npc_unity_invite_key(ctx.user.idpk, invited_user.idpk)
        if await redis.get(invite_key):
            return ActionResponse(status="skipped", summary="invite_already_sent", error_code="invite_already_sent")

        await redis.set(invite_key, str(unity.idpk), ex=settings.unity_invite_ttl_seconds)
        await bot.send_message(
            chat_id=invited_user.id_user,
            text=(
                f"NPC {ctx.user.nickname} приглашает вас в объединение \"{unity.name}\". "
                f"Доход объединения: {current_unity['income']} RUB/мин."
            ),
            reply_markup=await ik_npc_unity_invitation(
                unity_idpk=unity.idpk,
                owner_idpk=ctx.user.idpk,
            ),
        )
        return ActionResponse(status="ok", summary=f"recruit_invite:{invited_user.nickname}")

class UpgradeUnityLevelParams(BaseModel):
    pass

class UpgradeUnityLevelAction(BaseAction[UpgradeUnityLevelParams]):
    name = "upgrade_unity_level"
    description = "Upgrade the level of your unity."
    params_model = UpgradeUnityLevelParams

    async def execute(self, ctx: ActionContext, params: UpgradeUnityLevelParams) -> ActionResponse:
        current_unity = ctx.observation["unity"]["current"]
        if not current_unity or not current_unity["is_owner"]:
            return ActionResponse(status="skipped", summary="not_unity_owner")
        unity = await ctx.session.get(Unity, current_unity["idpk"])
        if not unity or unity.level >= 3:
            return ActionResponse(status="skipped", summary="unity_max_level")
        if not await can_upgrade_unity(session=ctx.session, unity=unity):
            return ActionResponse(status="skipped", summary="unity_conditions_not_met")
        unity.level += 1
        await tools.sync_user_income(session=ctx.session, user=ctx.user)
        return ActionResponse(status="ok", summary=f"upgrade_unity_level:{unity.level}")

class ReviewUnityRequestParams(BaseModel):
    idpk_user: int
    decision: Literal["accept", "reject"] = Field(default="accept")

class ReviewUnityRequestAction(BaseAction[ReviewUnityRequestParams]):
    name = "review_unity_request"
    description = "Accept or reject a request to join your unity."
    params_model = ReviewUnityRequestParams

    async def execute(self, ctx: ActionContext, params: ReviewUnityRequestParams) -> ActionResponse:
        current_unity = ctx.observation["unity"].get("current")
        if not current_unity or not current_unity.get("is_owner"):
            return ActionResponse(status="skipped", summary="not_unity_owner")

        applicant_idpk = params.idpk_user
        decision = params.decision

        request = await ctx.session.scalar(
            select(RequestToUnity).where(
                and_(
                    RequestToUnity.idpk_user == applicant_idpk,
                    RequestToUnity.idpk_unity_owner == ctx.user.idpk,
                )
            )
        )
        if not request:
            return ActionResponse(status="skipped", summary="unity_request_not_found", error_code="unity_request_not_found")

        applicant = await ctx.session.get(User, applicant_idpk)
        unity = await ctx.session.get(Unity, current_unity["idpk"])
        if not applicant or not unity:
            await ctx.session.delete(request)
            return ActionResponse(status="skipped", summary="unity_request_stale")

        if applicant.current_unity:
            await ctx.session.delete(request)
            return ActionResponse(status="skipped", summary="applicant_already_in_unity", error_code="applicant_already_in_unity")

        if decision == "reject":
            await ctx.session.delete(request)
            return ActionResponse(status="ok", summary=f"reject_unity_request:{applicant.nickname}")

        await add_unity_member(session=ctx.session, unity=unity, member_idpk=applicant.idpk)
        applicant.current_unity = f"member:{unity.idpk}"
        await tools.sync_user_income(session=ctx.session, user=applicant)
        await ctx.session.delete(request)
        return ActionResponse(status="ok", summary=f"accept_unity_request:{applicant.nickname}")

class ExitFromUnityParams(BaseModel):
    pass

class ExitFromUnityAction(BaseAction[ExitFromUnityParams]):
    name = "exit_from_unity"
    description = "Exit from your current unity."
    params_model = ExitFromUnityParams

    async def execute(self, ctx: ActionContext, params: ExitFromUnityParams) -> ActionResponse:
        current_unity = ctx.user.current_unity
        if not current_unity:
            return ActionResponse(status="skipped", summary="not_in_unity")

        unity_idpk = int(get_unity_idpk(current_unity) or 0)
        unity = await ctx.session.get(Unity, unity_idpk)
        if not unity:
            ctx.user.current_unity = None
            await tools.sync_user_income(session=ctx.session, user=ctx.user)
            return ActionResponse(status="ok", summary="exit_unity:stale")

        if unity.idpk_user != ctx.user.idpk:
            await remove_unity_member(session=ctx.session, unity=unity, member_idpk=ctx.user.idpk)
            ctx.user.current_unity = None
            await tools.sync_user_income(session=ctx.session, user=ctx.user)
            return ActionResponse(status="ok", summary="exit_unity:member")

        ctx.user.current_unity = None
        idpk_next_owner = await pop_next_unity_owner(session=ctx.session, unity=unity)
        if idpk_next_owner:
            next_owner: User = await ctx.session.get(User, idpk_next_owner)
            if next_owner:
                next_owner.current_unity = f"owner:{unity.idpk}"
                unity.idpk_user = next_owner.idpk
                await tools.sync_user_income(session=ctx.session, user=next_owner)
            await tools.sync_user_income(session=ctx.session, user=ctx.user)
            return ActionResponse(status="ok", summary="exit_unity:owner_promoted")

        await tools.sync_user_income(session=ctx.session, user=ctx.user)
        await ctx.session.delete(unity)
        return ActionResponse(status="ok", summary="exit_unity:deleted")

class ContributeClanProjectParams(BaseModel):
    rub: int = Field(default=0, ge=0)
    usd: int = Field(default=0, ge=0)

class ContributeClanProjectAction(BaseAction[ContributeClanProjectParams]):
    name = "contribute_clan_project"
    description = "Contribute RUB and USD to the current clan project."
    params_model = ContributeClanProjectParams

    async def execute(self, ctx: ActionContext, params: ContributeClanProjectParams) -> ActionResponse:
        unity_idpk = int(get_unity_idpk(ctx.user.current_unity) or 0)
        if not unity_idpk:
            return ActionResponse(status="error", summary="no_unity")
        unity = await ctx.session.get(Unity, unity_idpk)
        if not unity:
            return ActionResponse(status="error", summary="unity_not_found")

        rub = params.rub
        usd = params.usd
        if rub == 0 and usd == 0:
            rub = min(int(ctx.user.rub or 0), 50_000)
            usd = min(int(ctx.user.usd or 0), 5_000)

        ok, msg, project = await contribute_to_project(
            session=ctx.session,
            user=ctx.user,
            unity=unity,
            rub=rub,
            usd=usd,
        )
        if not ok:
            return ActionResponse(status="skipped", summary=f"project_contribution_skipped:{msg}")

        pr = project.get("progress", {})
        tg = project.get("target", {})
        return ActionResponse(
            status="ok",
            summary=(
                f"project_contribution:+{rub}RUB +{usd}USD | "
                f"progress rub {int(pr.get('rub', 0))}/{int(tg.get('rub', 0))}, "
                f"usd {int(pr.get('usd', 0))}/{int(tg.get('usd', 0))}"
            ),
        )

class OpenClanChestParams(BaseModel):
    chest_type: Literal["common", "rare", "epic", "best"] = Field(default="best")

class OpenClanChestAction(BaseAction[OpenClanChestParams]):
    name = "open_clan_chest"
    description = "Open a clan chest of a specific type."
    params_model = OpenClanChestParams

    async def execute(self, ctx: ActionContext, params: OpenClanChestParams) -> ActionResponse:
        chest_type = params.chest_type
        kwargs = {"open_common": 0, "open_rare": 0, "open_epic": 0}
        if chest_type == "common":
            kwargs["open_common"] = 1
        elif chest_type == "rare":
            kwargs["open_rare"] = 1
        elif chest_type == "epic":
            kwargs["open_epic"] = 1

        ok, msg, balance, rewards = await open_user_chests(
            session=ctx.session, user=ctx.user, **kwargs
        )
        if not ok:
            return ActionResponse(status="skipped", summary=f"open_chest_skipped:{msg}")

        return ActionResponse(
            status="ok",
            summary=(
                f"open_chest:{chest_type} +{int(rewards.get('rub', 0))}RUB +{int(rewards.get('usd', 0))}USD | "
                f"left c:{int(balance.get('common', 0))} r:{int(balance.get('rare', 0))} e:{int(balance.get('epic', 0))}"
            ),
        )

class SendChatTransferParams(BaseModel):
    currency: Literal["usd", "rub"] = Field(default="usd")
    amount: int = Field(ge=1)
    pieces: int = Field(default=1, ge=1)

class SendChatTransferAction(BaseAction[SendChatTransferParams]):
    name = "send_chat_transfer"
    description = "Start a money giveaway in the chat."
    params_model = SendChatTransferParams

    async def execute(self, ctx: ActionContext, params: SendChatTransferParams) -> ActionResponse:
        currency = params.currency
        amount = params.amount
        pieces = params.pieces
        if pieces > amount:
            pieces = amount
        if pieces > 500:
            pieces = 500

        balance = int(ctx.user.usd) if currency == "usd" else int(ctx.user.rub)
        if balance < amount:
            return ActionResponse(status="skipped", summary="not_enough_currency")

        one_piece = max(1, amount // pieces)
        total_spend = one_piece * pieces
        if currency == "usd":
            ctx.user.usd -= total_spend
            ctx.user.amount_expenses_usd += total_spend
        else:
            ctx.user.rub -= total_spend
            ctx.user.amount_expenses_rub += total_spend

        transfer = TransferMoney(
            id_transfer=gen_key(length=10),
            idpk_user=ctx.user.idpk,
            currency=currency,
            one_piece_sum=one_piece,
            pieces=pieces,
            status=True,
            source_chat_id=CHAT_ID,
        )
        ctx.session.add(transfer)
        await ctx.session.flush()

        keyboard = (
            await ik_get_money(
                one_piece=f"{one_piece}{'$' if currency == 'usd' else '₽'}",
                remain_pieces=pieces,
                idpk_tr=transfer.idpk,
            )
            if pieces > 1
            else await ik_get_money_one_piece(idpk_tr=transfer.idpk)
        )
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"{ctx.user.nickname} устроил раздачу: {total_spend}{'$' if currency == 'usd' else '₽'} "
                f"на {pieces} частей. Забирайте 👇"
            ),
            reply_markup=keyboard,
        )
        return ActionResponse(
            status="ok",
            summary=f"chat_transfer:{currency}:{total_spend}:{pieces}",
        )

class ClaimChatTransferParams(BaseModel):
    idpk_tr: int

class ClaimChatTransferAction(BaseAction[ClaimChatTransferParams]):
    name = "claim_chat_transfer"
    description = "Claim a piece of a chat money giveaway."
    params_model = ClaimChatTransferParams

    async def execute(self, ctx: ActionContext, params: ClaimChatTransferParams) -> ActionResponse:
        idpk_tr = params.idpk_tr
        tr = await ctx.session.get(TransferMoney, idpk_tr)
        if not tr or not tr.status:
            return ActionResponse(status="skipped", summary="transfer_not_found")
        tr_chat_id = int(getattr(tr, "source_chat_id", 0) or 0)
        if tr_chat_id != 0 and tr_chat_id != int(CHAT_ID):
            return ActionResponse(status="skipped", summary="transfer_not_official_chat")
        if int(tr.idpk_user) == int(ctx.user.idpk):
            return ActionResponse(status="skipped", summary="own_transfer")
        if int(tr.pieces or 0) <= 0:
            return ActionResponse(status="skipped", summary="transfer_empty")

        if await in_used(session=ctx.session, idpk_tr=tr.idpk, idpk_user=ctx.user.idpk):
            return ActionResponse(status="skipped", summary="transfer_already_used")

        await add_user_to_used(session=ctx.session, idpk_tr=tr.idpk, idpk_user=ctx.user.idpk)
        await add_to_currency(self=ctx.user, currency=tr.currency, amount=int(tr.one_piece_sum))
        tr.pieces -= 1

        if tr.pieces <= 0:
            tr.status = False

        if tr.id_mess:
            cur = "$" if tr.currency == "usd" else "₽"
            keyboard = (
                await ik_get_money(
                    one_piece=f"{int(tr.one_piece_sum):,d}{cur}",
                    remain_pieces=int(tr.pieces),
                    idpk_tr=tr.idpk,
                )
                if int(tr.pieces) > 0
                else None
            )
            with contextlib.suppress(Exception):
                await bot.edit_message_reply_markup(
                    reply_markup=keyboard,
                    **get_id_for_edit_message(str(tr.id_mess)),
                )

        return ActionResponse(
            status="ok",
            summary=f"claim_chat_transfer:{tr.currency}:{int(tr.one_piece_sum)}",
        )

class CreateChatGameParams(BaseModel):
    game_type: str = Field(default="🎲")
    amount_gamers: int = Field(default=3, ge=2, le=80)
    amount_award: int = Field(ge=1)
    currency: Literal["usd", "rub"] = Field(default="usd")
    amount_moves: int = Field(default=5, ge=1)

class CreateChatGameAction(BaseAction[CreateChatGameParams]):
    name = "create_chat_game"
    description = "Create a new chat game."
    params_model = CreateChatGameParams

    async def execute(self, ctx: ActionContext, params: CreateChatGameParams) -> ActionResponse:
        game_type = params.game_type
        if game_type not in games:
            return ActionResponse(status="skipped", summary="bad_game_type")

        amount_gamers = params.amount_gamers
        amount_award = params.amount_award
        currency = params.currency
        
        balance = int(ctx.user.usd) if currency == "usd" else int(ctx.user.rub)
        if balance < amount_award:
            return ActionResponse(status="skipped", summary="not_enough_currency")

        if currency == "usd":
            ctx.user.usd -= amount_award
            ctx.user.amount_expenses_usd += amount_award
        else:
            ctx.user.rub -= amount_award
            ctx.user.amount_expenses_rub += amount_award

        sec_to_expire_game = int(
            await get_value(session=ctx.session, value_name="SEC_TO_EXPIRE_GAME")
        )
        game = Game(
            id_game=f"game_{gen_key(length=12)}",
            idpk_user=ctx.user.idpk,
            type_game=game_type,
            amount_gamers=amount_gamers,
            amount_award=Decimal(amount_award),
            currency_award=currency,
            end_date=datetime.now() + timedelta(seconds=sec_to_expire_game),
            amount_moves=params.amount_moves,
            activate=True,
            source_chat_id=CHAT_ID,
        )
        ctx.session.add(game)
        await ctx.session.flush()

        link = await create_start_link(bot=bot, payload=game.id_game)
        msg = await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"{ctx.user.nickname} создал мини-игру {game_type}: "
                f"игроков {amount_gamers}, приз {amount_award}{'$' if currency == 'usd' else '₽'}."
            ),
            reply_markup=await ik_start_created_game(
                link=link,
                current_gamers=0,
                total_gamers=amount_gamers,
            ),
            disable_web_page_preview=True,
        )
        game.id_mess = str(msg.message_id)
        return ActionResponse(status="ok", summary=f"create_chat_game:{game.id_game}")

class JoinChatGameParams(BaseModel):
    id_game: str

class JoinChatGameAction(BaseAction[JoinChatGameParams]):
    name = "join_chat_game"
    description = "Join an existing chat game."
    params_model = JoinChatGameParams

    async def execute(self, ctx: ActionContext, params: JoinChatGameParams) -> ActionResponse:
        id_game = params.id_game.strip()
        if not id_game:
            return ActionResponse(status="skipped", summary="id_game_missing")

        game = await ctx.session.scalar(select(Game).where(Game.id_game == id_game))
        if not game or game.end:
            return ActionResponse(status="skipped", summary="game_not_found")
        if game.idpk_user == ctx.user.idpk:
            return ActionResponse(status="skipped", summary="game_owner_cannot_join")

        gamer = await ctx.session.scalar(
            select(Gamer).where(Gamer.id_game == id_game, Gamer.idpk_gamer == ctx.user.idpk)
        )
        if gamer:
            return ActionResponse(status="skipped", summary="already_joined")

        active_game = await ctx.session.scalar(
            select(Gamer).where(Gamer.idpk_gamer == ctx.user.idpk, Gamer.game_end == False)  # noqa: E712
        )
        if active_game:
            return ActionResponse(status="skipped", summary="has_active_game")

        current_gamers = int(
            await ctx.session.scalar(
                select(func.count()).select_from(Gamer).where(Gamer.id_game == id_game)
            )
            or 0
        )
        if current_gamers >= int(game.amount_gamers):
            return ActionResponse(status="skipped", summary="game_full")

        ctx.session.add(
            Gamer(id_game=id_game, idpk_gamer=ctx.user.idpk, moves=int(game.amount_moves))
        )
        return ActionResponse(status="ok", summary=f"join_chat_game:{id_game}")

# Registry and registrations
ActionRegistry.register(WaitAction())
ActionRegistry.register(ChangeOwnMoodAction())
ActionRegistry.register(SetTacticalFocusAction())
ActionRegistry.register(SendNpcSignalAction())
ActionRegistry.register(ClaimDailyBonusAction())
ActionRegistry.register(InvestForIncomeAction())
ActionRegistry.register(InvestForTopAnimalsAction())
ActionRegistry.register(ExchangeBankAction())
ActionRegistry.register(BuyAviaryAction())
ActionRegistry.register(BuyRarityAnimalAction())
ActionRegistry.register(BuyMerchantDiscountOfferAction())
ActionRegistry.register(BuyMerchantRandomOfferAction())
ActionRegistry.register(BuyMerchantTargetedOfferAction())
ActionRegistry.register(CreateItemAction())
ActionRegistry.register(ActivateItemAction())
ActionRegistry.register(DeactivateItemAction())
ActionRegistry.register(SellItemAction())
ActionRegistry.register(OptimizeItemsAction())
ActionRegistry.register(UpgradeItemAction())
ActionRegistry.register(MergeItemsAction())
ActionRegistry.register(CreateUnityAction())
ActionRegistry.register(JoinBestUnityAction())
ActionRegistry.register(RecruitTopPlayerAction())
ActionRegistry.register(UpgradeUnityLevelAction())
ActionRegistry.register(ReviewUnityRequestAction())
ActionRegistry.register(ExitFromUnityAction())
ActionRegistry.register(ContributeClanProjectAction())
ActionRegistry.register(OpenClanChestAction())
ActionRegistry.register(SendChatTransferAction())
ActionRegistry.register(ClaimChatTransferAction())
ActionRegistry.register(CreateChatGameAction())
ActionRegistry.register(JoinChatGameAction())
