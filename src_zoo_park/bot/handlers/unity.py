import contextlib
from datetime import datetime, timedelta

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import any_state
from aiogram.types import CallbackQuery, Message
from bot.callbacks import (
    NpcUnityInviteDecisionCallback,
    UnityBackCallback,
    UnityPageCallback,
    UnityRequestDecision,
    UnityRequestDecisionCallback,
    UnityViewCallback,
)
from bot.filters import GetTextButton
from bot.keyboards import (
    ik_menu_unity_to_join,
    ik_npc_unity_invitation,
    ik_unity_invitation,
    ik_unity_options,
    ik_unity_send_request,
    rk_back,
    rk_main_menu,
    rk_unity_menu,
)
from init_db_redis import redis
from npc_agent.schedule import wake_npc_now
from bot.states import UserState
from db import RequestToUnity, Unity, User
from db.structured_state import (
    add_unity_member,
    pop_next_unity_owner,
    remove_unity_member,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    count_page_unity,
    disable_not_main_window,
    get_text_message,
    get_total_number_animals,
    get_value,
    has_special_characters_name,
    income_,
    is_unique_name,
    mention_html,
    shorten_whitespace_name_unity,
)

flags = {"throttling_key": "default"}
router = Router()


def npc_unity_invite_key(owner_idpk: int, target_idpk: int) -> str:
    return f"npc_unity_invite:{owner_idpk}:{target_idpk}"


@router.message(UserState.main_menu, GetTextButton("unity"), flags=flags)
async def unity_menu(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await disable_not_main_window(data=await state.get_data(), message=message)
    if user.current_unity:
        idpk_unity = int(user.current_unity.split(":")[-1])
        unity: Unity = await session.get(Unity, idpk_unity)
        await state.set_state(UserState.unity_menu)
        await state.update_data(idpk_unity=unity.idpk)
        await message.answer(
            text=await get_text_message("unity_menu"),
            reply_markup=await rk_unity_menu(),
        )
        return
    PRICE_FOR_CREATE_UNITY = await get_value(
        session=session, value_name="PRICE_FOR_CREATE_UNITY"
    )
    await state.update_data(price_fcu=PRICE_FOR_CREATE_UNITY)
    msg = await message.answer(
        text=await get_text_message("unity_options"),
        reply_markup=await ik_unity_options(price_create=PRICE_FOR_CREATE_UNITY),
    )
    await state.update_data(active_window=msg.message_id)


@router.message(UserState.unity_menu, GetTextButton("back"))
async def back_to_main_menu(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await disable_not_main_window(data=await state.get_data(), message=message)
    await state.set_state(UserState.main_menu)
    await message.answer(
        text=await get_text_message("main_menu"),
        reply_markup=await rk_main_menu(user_id=user.id_user),
    )


@router.callback_query(UserState.main_menu, F.data == "create_unity")
async def create_unity(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    if user.usd < data["price_fcu"]:
        await query.answer(
            await get_text_message("not_enough_money_to_create"),
            show_alert=True,
        )
        return
    await state.set_state(UserState.enter_name_unity_step)
    await query.message.delete()
    await query.message.answer(
        text=await get_text_message("enter_name_unity"),
        reply_markup=await rk_back(),
    )


@router.message(UserState.enter_name_unity_step, GetTextButton("back"))
async def back_to_main_menu_2(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await state.set_state(UserState.main_menu)
    await message.answer(
        text=await get_text_message("main_menu"),
        reply_markup=await rk_main_menu(user_id=user.id_user),
    )


@router.message(UserState.enter_name_unity_step)
async def get_name_unity(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    LIMIT_LENGTH_MAX = await get_value(
        session=session, value_name="NAME_UNITY_LENGTH_MAX"
    )
    LIMIT_LENGTH_MIN = await get_value(
        session=session, value_name="NAME_UNITY_LENGTH_MIN"
    )
    if len(message.text) > LIMIT_LENGTH_MAX:
        await message.answer(text=await get_text_message("name_unity_too_long"))
        return
    if len(message.text) < LIMIT_LENGTH_MIN:
        await message.answer(text=await get_text_message("name_unity_too_short"))
        return
    if chars := await has_special_characters_name(message.text):
        await message.answer(
            text=await get_text_message(
                "name_unity_has_special_characters", chars=chars
            )
        )
        return
    if not await is_unique_name(session=session, nickname=message.text):
        await message.answer(text=await get_text_message("nickname_not_unique"))
        return
    data = await state.get_data()
    user.usd -= data["price_fcu"]
    user.amount_expenses_usd += data["price_fcu"]
    name_unity = await shorten_whitespace_name_unity(message.text)
    unity = Unity(idpk_user=user.idpk, name=name_unity)
    session.add(unity)
    await session.flush()
    user.current_unity = f"owner:{unity.idpk}"
    await session.commit()
    await state.set_state(UserState.unity_menu)
    await state.update_data(idpk_unity=unity.idpk)
    await message.answer(
        text=await get_text_message("unity_menu"),
        reply_markup=await rk_unity_menu(),
    )


@router.callback_query(UserState.main_menu, F.data == "join_to_unity")
async def join_to_unity(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    q_page = await count_page_unity(session=session)
    if q_page == 0:
        await query.answer(
            text=await get_text_message("no_unity_to_join"), show_alert=True
        )
        return
    await state.update_data(q_page=q_page, page=1)
    await query.message.edit_text(
        text=await get_text_message("join_to_unity_menu"),
        reply_markup=await ik_menu_unity_to_join(session=session, page=1),
    )


@router.callback_query(UserState.main_menu, UnityPageCallback.filter())
async def process_turn_unity(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: UnityPageCallback,
) -> None:
    data = await state.get_data()
    page = data["page"]
    side = callback_data.direction.value
    if side == "left":
        page = page - 1 if page > 1 else data["q_page"]
    else:
        page = page + 1 if page < data["q_page"] else 1
    await state.update_data(page=page)
    with contextlib.suppress(Exception):
        await query.message.edit_reply_markup(
            reply_markup=await ik_menu_unity_to_join(session=session, page=page)
        )


@router.callback_query(UserState.main_menu, UnityBackCallback.filter())
async def process_back_to_menu_all_unity(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: UnityBackCallback,
) -> None:
    back_to = callback_data.target.value
    match back_to:
        case "to_menu_unity":
            data = await state.get_data()
            await query.message.edit_text(
                text=await get_text_message("unity_options"),
                reply_markup=await ik_unity_options(data["price_fcu"]),
            )
        case "to_all_unity":
            q_page = await count_page_unity(session=session)
            await state.update_data(q_page=q_page, page=1)
            await query.message.edit_text(
                text=await get_text_message("join_to_unity_menu"),
                reply_markup=await ik_menu_unity_to_join(session=session, page=1),
            )


@router.callback_query(UserState.main_menu, UnityViewCallback.filter())
async def process_viewing_unity_bio(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: UnityViewCallback,
) -> None:
    idpk_owner = callback_data.owner_idpk
    unity = await session.scalar(select(Unity).where(Unity.idpk_user == idpk_owner))
    owner = await session.get(User, idpk_owner)
    await state.update_data(idpk_owner=idpk_owner)
    await query.message.edit_text(
        text=await get_text_message(
            "description_unity",
            name_=unity.format_name,
            lvl=unity.level,
            nickname_owner=owner.nickname,
            owner_income=await income_(session=session, user=owner),
        ),
        reply_markup=await ik_unity_send_request(),
    )


@router.callback_query(UserState.main_menu, F.data == "request_unity")
async def process_send_request(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
) -> None:
    if await session.scalar(
        select(RequestToUnity).where(RequestToUnity.idpk_user == user.idpk)
    ):
        await query.answer(
            text=await get_text_message("already_send_request"), show_alert=True
        )
        return
    data = await state.get_data()
    unity_owner: User = await session.get(User, data["idpk_owner"])
    await query.message.edit_text(
        text=await get_text_message("request_send"),
        reply_markup=None,
    )
    MIN_TO_END_REQUEST = await get_value(
        session=session, value_name="MIN_TO_END_REQUEST"
    )
    date_request_end = datetime.now() + timedelta(minutes=MIN_TO_END_REQUEST)
    request = RequestToUnity(
        idpk_user=user.idpk,
        idpk_unity_owner=unity_owner.idpk,
        date_request=datetime.now(),
        date_request_end=date_request_end,
    )
    session.add(request)
    if unity_owner.id_user < 0:
        await wake_npc_now(
            session=session,
            user_idpk=unity_owner.idpk,
            reason=f"unity_request:{user.idpk}",
        )
    await session.commit()
    await query.bot.send_message(
        chat_id=unity_owner.id_user,
        text=await get_text_message(
            "request_add_to_unity",
            nickname=mention_html(id_user=user.id_user, name=user.nickname),
            usd=user.usd,
            rub=user.rub,
            animals=await get_total_number_animals(self=user, session=session),
            income=await income_(session=session, user=user),
        ),
        reply_markup=await ik_unity_invitation(user.idpk),
    )


@router.callback_query(
    StateFilter(any_state),
    UnityRequestDecisionCallback.filter(F.decision == UnityRequestDecision.accept),
)
async def process_accept_to_unity(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: UnityRequestDecisionCallback,
) -> None:
    unity: Unity = await session.scalar(
        select(Unity).where(Unity.idpk_user == user.idpk)
    )
    member: int = callback_data.user_idpk
    member: User = await session.get(User, member)
    member.current_unity = f"member:{unity.idpk}"
    r = await session.scalar(
        select(RequestToUnity).where(RequestToUnity.idpk_user == member.idpk)
    )
    if r:
        await session.delete(r)
    await add_unity_member(session=session, unity=unity, member_idpk=member.idpk)
    await tools.sync_user_income(session=session, user=member)
    await session.commit()
    await query.message.edit_text(
        text=await get_text_message("request_accepted"),
        reply_markup=None,
    )
    await query.bot.send_message(
        chat_id=member.id_user,
        text=await get_text_message("request_accepted_to_unity"),
    )


@router.callback_query(
    StateFilter(any_state),
    UnityRequestDecisionCallback.filter(F.decision == UnityRequestDecision.reject),
)
async def process_rejected_to_unity(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: UnityRequestDecisionCallback,
) -> None:
    member: int = callback_data.user_idpk
    member: User = await session.get(User, member)
    r = await session.scalar(
        select(RequestToUnity).where(RequestToUnity.idpk_user == member.idpk)
    )
    await session.delete(r)
    await session.commit()
    await query.message.edit_text(
        text=await get_text_message("request_rejected"),
        reply_markup=None,
    )
    await query.bot.send_message(
        chat_id=member.id_user,
        text=await get_text_message("request_rejected_to_unity"),
    )


@router.callback_query(
    StateFilter(any_state),
    NpcUnityInviteDecisionCallback.filter(F.decision == UnityRequestDecision.accept),
)
async def accept_npc_unity_invite(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: NpcUnityInviteDecisionCallback,
) -> None:
    if user.current_unity:
        await query.answer(text="Вы уже состоите в объединении", show_alert=True)
        return

    unity = await session.get(Unity, callback_data.unity_idpk)
    owner = await session.get(User, callback_data.owner_idpk)
    if not unity or not owner or unity.idpk_user != owner.idpk:
        await query.answer(text="Приглашение больше неактуально", show_alert=True)
        return

    invite_key = npc_unity_invite_key(owner.idpk, user.idpk)
    if not await redis.get(invite_key):
        await query.answer(
            text="Приглашение истекло или было отозвано", show_alert=True
        )
        return

    await add_unity_member(session=session, unity=unity, member_idpk=user.idpk)
    user.current_unity = f"member:{unity.idpk}"
    await redis.delete(invite_key)
    await wake_npc_now(
        session=session,
        user_idpk=owner.idpk,
        reason=f"npc_invite_accepted:{user.idpk}",
    )
    await tools.sync_user_income(session=session, user=user)
    await session.commit()
    await query.message.edit_text("Вы вступили в объединение NPC")
    await query.answer(text="Приглашение принято", show_alert=False)


@router.callback_query(
    StateFilter(any_state),
    NpcUnityInviteDecisionCallback.filter(F.decision == UnityRequestDecision.reject),
)
async def reject_npc_unity_invite(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: NpcUnityInviteDecisionCallback,
) -> None:
    invite_key = npc_unity_invite_key(callback_data.owner_idpk, user.idpk)
    await redis.delete(invite_key)
    await wake_npc_now(
        session=session,
        user_idpk=callback_data.owner_idpk,
        reason=f"npc_invite_rejected:{user.idpk}",
    )
    await session.commit()
    await query.message.edit_text("Вы отклонили приглашение в объединение")
    await query.answer(text="Приглашение отклонено", show_alert=False)


@router.message(UserState.unity_menu, GetTextButton("exit_from_unity"))
async def exit_from_unity(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    unity = await session.get(Unity, data["idpk_unity"])
    # если не владелец объединения
    if unity.idpk_user != user.idpk:
        await remove_unity_member(session=session, unity=unity, member_idpk=user.idpk)
        user.current_unity = None
        owner = await session.get(User, unity.idpk_user)
        if owner and owner.id_user < 0:
            await wake_npc_now(
                session=session,
                user_idpk=owner.idpk,
                reason=f"unity_member_left:{user.idpk}",
            )
        await state.set_state(UserState.main_menu)
        await tools.sync_user_income(session=session, user=user)
        await session.commit()
        await message.answer(
            text=await get_text_message("exit_from_unity_text"), reply_markup=None
        )
        await message.answer(
            text=await get_text_message("main_menu"),
            reply_markup=await rk_main_menu(user_id=user.id_user),
        )
        return
    # если владелец объединения
    user.current_unity = None
    await tools.sync_user_income(session=session, user=user)
    idpk_next_owner = await pop_next_unity_owner(session=session, unity=unity)
    if idpk_next_owner:
        next_owner: User = await session.get(User, idpk_next_owner)
        next_owner.current_unity = f"owner:{unity.idpk}"
        await tools.sync_user_income(session=session, user=next_owner)
        unity.idpk_user = next_owner.idpk
        if next_owner.id_user < 0:
            await wake_npc_now(
                session=session,
                user_idpk=next_owner.idpk,
                reason=f"unity_owner_promoted:{unity.idpk}",
            )
        await message.answer(
            text=await get_text_message("exit_from_unity_text"), reply_markup=None
        )
        await message.bot.send_message(
            chat_id=next_owner.id_user,
            text=await get_text_message("you_new_owner_unity"),
        )
    else:
        await session.delete(unity)
        await message.answer(
            text=await get_text_message("unity_deleted"), reply_markup=None
        )
    await state.set_state(UserState.main_menu)
    await message.answer(
        text=await get_text_message("main_menu"),
        reply_markup=await rk_main_menu(user_id=user.id_user),
    )
    await session.commit()
