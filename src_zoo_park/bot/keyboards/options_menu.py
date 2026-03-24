from itertools import islice

import tools
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from bot.callbacks import (
    AccountBackCallback,
    AccountBackTarget,
    AccountItemPageCallback,
    AccountItemViewCallback,
    AviaryBackCallback,
    AviaryChoiceCallback,
    AviaryQuantityCallback,
    CalculatorRateCallback,
    Direction,
    ForgeBackCallback,
    ForgeBackTarget,
    ForgeItemSelectCallback,
    ForgeItemsPageCallback,
    ForgePageMode,
    NpcUnityInviteDecisionCallback,
    RandomMerchantAnimalCallback,
    RandomMerchantBackCallback,
    RandomMerchantBackTarget,
    RandomMerchantOfferCallback,
    RandomMerchantQuantityCallback,
    RarityShopAnimalCallback,
    RarityShopBackCallback,
    RarityShopBackTarget,
    RarityShopQuantityCallback,
    RarityShopRarityCallback,
    RarityShopSwitchCallback,
    SupportAction,
    SupportTakeCallback,
    TransferActivateCallback,
    WorkshopBackCallback,
    UnityBackCallback,
    UnityBackTarget,
    UnityMemberPageCallback,
    UnityMemberViewCallback,
    UnityMembersBackCallback,
    UnityPageCallback,
    UnityRequestDecision,
    UnityRequestDecisionCallback,
    UnityViewCallback,
)
from game_variables import colors_rarities, rarities
from sqlalchemy.ext.asyncio import AsyncSession


async def ik_merchant_menu(
    price: int,
    first_offer_bought: bool,
    quantity_animals: int = None,
    name_animal: str = None,
    discount: int = None,
    price_with_discount: int = None,
):
    builder = InlineKeyboardBuilder()
    if not first_offer_bought:
        builder.button(
            text=await tools.get_text_button(
                "first_offer",
                qa=quantity_animals,
                na=name_animal,
                dnt=discount,
                pwd=price_with_discount,
            ),
            callback_data=RandomMerchantOfferCallback(offer=1),
        )
    builder.button(
        text=await tools.get_text_button("second_offer", p=price),
        callback_data=RandomMerchantOfferCallback(offer=2),
    )
    builder.button(
        text=await tools.get_text_button("third_offer"),
        callback_data=RandomMerchantOfferCallback(offer=3),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_choice_animal_rmerchant(session: AsyncSession):
    builder = InlineKeyboardBuilder()
    all_animals = await tools.get_all_animals(session)
    for animal in all_animals:
        builder.button(
            text=animal.name,
            callback_data=RandomMerchantAnimalCallback(
                animal=animal.code_name.strip("-")
            ),
        )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=RandomMerchantBackCallback(
            target=RandomMerchantBackTarget.all_offers
        ),
    )
    quantity_names = len(all_animals)
    aj = 2
    if quantity_names >= aj:
        adjust = [aj for _ in range(quantity_names // aj)]
        remains = quantity_names % aj
        adjust = adjust + [remains] if remains > 0 else adjust
    else:
        adjust = [quantity_names]
    builder.adjust(*adjust, 1)
    return builder.as_markup()


async def ik_choice_quantity_animals_rmerchant(
    session: AsyncSession, animal_price: int, magic_count_animal: int
):
    builder = InlineKeyboardBuilder()
    all_quantity_animals = await tools.fetch_and_parse_str_value(
        session=session, value_name="QUANTITIES_FOR_RANDOM_MERCHANT"
    )
    prices = [animal_price * q for q in all_quantity_animals]
    for quantity_animal, price in zip(all_quantity_animals, prices):
        builder.button(
            text=await tools.get_text_button(
                "pattern_quantity_animals", qa=quantity_animal, pr=price
            ),
            callback_data=RandomMerchantQuantityCallback(quantity=quantity_animal),
        )
    if magic_count_animal != 0:
        pr = magic_count_animal * animal_price
        builder.button(
            text=await tools.get_text_button(
                "pattern_quantity_animals_magic", qa=magic_count_animal, pr=pr
            ),
            callback_data=RandomMerchantQuantityCallback(quantity=magic_count_animal),
        )
    builder.button(
        text=await tools.get_text_button("custom_quantity_animals"),
        callback_data="cqa",  # custom_quantity_animals
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=RandomMerchantBackCallback(
            target=RandomMerchantBackTarget.choice_animal
        ),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_choice_animal_rshop(session: AsyncSession):
    builder = InlineKeyboardBuilder()
    all_animals = await tools.get_all_animals(session)
    for animal in all_animals:
        builder.button(
            text=animal.name,
            callback_data=RarityShopAnimalCallback(animal=animal.code_name.strip("-")),
        )
    builder.adjust(2)
    return builder.as_markup()


async def ik_choice_rarity_rshop():
    builder = InlineKeyboardBuilder()
    for rarity, color_rarity in zip(rarities, colors_rarities):
        builder.button(
            text=color_rarity,
            callback_data=RarityShopRarityCallback(rarity=rarity),
        )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=RarityShopBackCallback(target=RarityShopBackTarget.choice_animal),
    )
    builder.adjust(4, 1)
    return builder.as_markup()


async def ik_choice_quantity_animals_rshop(
    session: AsyncSession, animal_price: int, magic_count_animal: int
):
    builder = InlineKeyboardBuilder()
    all_quantity_animals = await tools.fetch_and_parse_str_value(
        session=session, value_name="QUANTITIES_FOR_RARITY_SHOP"
    )
    prices = [animal_price * q for q in all_quantity_animals]
    builder.button(
        text=await tools.get_text_button("prev_rarity"),
        callback_data=RarityShopSwitchCallback(direction=Direction.left),
    )
    builder.button(
        text=await tools.get_text_button("next_rarity"),
        callback_data=RarityShopSwitchCallback(direction=Direction.right),
    )
    for quantity_animal, price in zip(all_quantity_animals, prices):
        builder.button(
            text=await tools.get_text_button(
                "pattern_quantity_animals_rshop",
                qa=quantity_animal,
                pr=tools.formatter.format_large_number(price),
            ),
            callback_data=RarityShopQuantityCallback(quantity=quantity_animal),
        )
    if magic_count_animal != 0:
        all_quantity_animals.append(magic_count_animal)
        pr = magic_count_animal * animal_price
        builder.button(
            text=await tools.get_text_button(
                "pattern_quantity_animals_magic",
                qa=magic_count_animal,
                pr=tools.formatter.format_large_number(pr),
            ),
            callback_data=RarityShopQuantityCallback(quantity=magic_count_animal),
        )
    builder.button(
        text=await tools.get_text_button("custom_quantity_animals"),
        callback_data="cqa_rshop",
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=RarityShopBackCallback(target=RarityShopBackTarget.choice_rarity),
    )
    builder.adjust(2, *[1 for _ in range(len(all_quantity_animals))], 1, 1)
    return builder.as_markup()


async def ik_buy_item(bought: bool):
    builder = InlineKeyboardBuilder()
    if not bought:
        builder.button(
            text=await tools.get_text_button("buy_item"), callback_data="buy_item"
        )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=WorkshopBackCallback(target="to_witems_menu"),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_forge_items_menu():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("create_item_info"),
        callback_data="create_item_info",
    )
    builder.button(
        text=await tools.get_text_button("merge_items_info"),
        callback_data="merge_items_info",
    )
    builder.button(
        text=await tools.get_text_button("up_lvl_item_info"),
        callback_data="up_lvl_item_info",
    )

    builder.adjust(1)
    return builder.as_markup()


async def ik_choice_aviary(
    session: AsyncSession,
):
    builder = InlineKeyboardBuilder()
    all_aviaries = await tools.get_name_and_code_name(session)
    for name, code_name in all_aviaries:
        builder.button(
            text=f"{name}",
            callback_data=AviaryChoiceCallback(aviary=code_name),
        )
    builder.adjust(1)
    return builder.as_markup()


async def ik_choice_quantity_aviary_avi(session: AsyncSession, aviary_price: int):
    builder = InlineKeyboardBuilder()
    all_quantity_aviaries = await tools.fetch_and_parse_str_value(
        session=session, value_name="QUANTITIES_FOR_AVIARIES"
    )
    prices = [aviary_price * q for q in all_quantity_aviaries]
    for quantity_aviary, price in zip(all_quantity_aviaries, prices):
        builder.button(
            text=await tools.get_text_button(
                "pattern_quantity_aviary_avi", qa=quantity_aviary, pr=price
            ),
            callback_data=AviaryQuantityCallback(quantity=quantity_aviary),
        )
    builder.button(
        text=await tools.get_text_button("custom_quantity_aviary"),
        callback_data="cqa_avi",
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=AviaryBackCallback(target="to_choice_aviary"),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_bank():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("exchange"), callback_data="exchange_bank"
    )
    builder.button(
        text=await tools.get_text_button("update"), callback_data="update_bank"
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_bank_modify():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("market_collapse"),
        callback_data="market_collapse",
    )
    builder.button(
        text=await tools.get_text_button("exchange"), callback_data="exchange_bank"
    )
    builder.button(
        text=await tools.get_text_button("update"), callback_data="update_bank"
    )
    builder.adjust(1)
    return builder.as_markup()


async def rk_exchange_bank():
    builder = ReplyKeyboardBuilder()
    builder.button(text=await tools.get_text_button("exchange_all_amount"))
    builder.button(text=await tools.get_text_button("back"))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)


async def ik_account_menu():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("account_animals"),
        callback_data="account_animals",
    )
    builder.button(
        text=await tools.get_text_button("account_aviaries"),
        callback_data="account_aviaries",
    )
    builder.button(text=await tools.get_text_button("items"), callback_data="items")
    builder.button(
        text=await tools.get_text_button("referrals_system"),
        callback_data="referrals_system",
    )
    builder.button(text="🎁 Открыть сундуки", callback_data="open_chests")
    builder.button(text="🏥 Ветеринар", callback_data="account_vet")
    builder.adjust(2, 2, 1, 1)
    return builder.as_markup()


# async def ik_menu_items(session: AsyncSession):
#     builder = InlineKeyboardBuilder()
#     all_name_items = await get_all_name_items(session=session)
#     builder.adjust(1)
#     return builder.as_markup()


async def ik_menu_items(
    session: AsyncSession,
    id_user: int,
    row_keyboard: int = None,
    page: int = 1,
    size_items: int = None,
) -> InlineKeyboardBuilder:
    row_keyboard = row_keyboard or await tools.get_row_items_for_kb(session=session)
    size_items = size_items or await tools.get_size_items_for_kb(session=session)
    builder = InlineKeyboardBuilder()
    start = (page - 1) * size_items
    stop = start + size_items
    items_data = await tools.get_items_data_to_kb(session=session, id_user=id_user)
    sliced_items_name = islice(items_data, start, stop)
    count_items = 0
    for name, id_item in sliced_items_name:
        builder.button(
            text=await tools.get_text_button(name="pattern_button_name_item", n=name),
            callback_data=AccountItemViewCallback(item_id=id_item),
        )
        count_items += 1
    whole_pairs = count_items // row_keyboard
    remainder = count_items % row_keyboard
    row = (
        [row_keyboard for _ in range(whole_pairs)] + [remainder]
        if remainder
        else [row_keyboard for _ in range(whole_pairs)]
    )

    builder.button(
        text=await tools.get_text_button("arrow_left"),
        callback_data=AccountItemPageCallback(direction=Direction.left),
    )
    builder.button(
        text=await tools.get_text_button("arrow_right"),
        callback_data=AccountItemPageCallback(direction=Direction.right),
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=AccountBackCallback(target=AccountBackTarget.account),
    )
    builder.adjust(*row, 2, 1)

    return builder.as_markup()


async def ik_item_activate_menu(is_activate: bool):
    builder = InlineKeyboardBuilder()
    match is_activate:
        case True:
            builder.button(
                text=await tools.get_text_button("item_deactivate"),
                callback_data="item_deactivate",
            )
        case False:
            builder.button(
                text=await tools.get_text_button("item_activate"),
                callback_data="item_activate",
            )
            builder.button(
                text=await tools.get_text_button("sell_item"), callback_data="sell_item"
            )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=AccountBackCallback(target=AccountBackTarget.items),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_unity_options(price_create: int):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("create_unity", price_create=price_create),
        callback_data="create_unity",
    )
    builder.button(
        text=await tools.get_text_button("join_to_unity"), callback_data="join_to_unity"
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_update_level_unity():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("update_level_unity"),
        callback_data="update_level_unity",
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_menu_unity_to_join(
    session: AsyncSession, row_keyboard: int = None, page: int = 1, size: int = None
) -> InlineKeyboardBuilder:
    row_keyboard = row_keyboard or await tools.get_row_unity_for_kb(session=session)
    size = size or await tools.get_size_unity_for_kb(session=session)
    builder = InlineKeyboardBuilder()
    start = (page - 1) * size
    stop = start + size
    data = await tools.get_unity_name_and_idpk(session=session)
    slice = islice(data, start, stop)
    counter = 0
    for name, idpk_owner in slice:
        builder.button(
            text=await tools.get_text_button(
                name="pattern_button_name_unity",
                n=name,
            ),
            callback_data=UnityViewCallback(owner_idpk=idpk_owner),
        )
        counter += 1
    whole_pairs = counter // row_keyboard
    remainder = counter % row_keyboard
    row = (
        [row_keyboard for _ in range(whole_pairs)] + [remainder]
        if remainder
        else [row_keyboard for _ in range(whole_pairs)]
    )

    builder.button(
        text=await tools.get_text_button("arrow_left"),
        callback_data=UnityPageCallback(direction=Direction.left),
    )
    builder.button(
        text=await tools.get_text_button("arrow_right"),
        callback_data=UnityPageCallback(direction=Direction.right),
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=UnityBackCallback(target=UnityBackTarget.menu),
    )
    builder.adjust(*row, 2, 1)

    return builder.as_markup()


async def ik_unity_send_request():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("send_a_request"),
        callback_data="request_unity",
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=UnityBackCallback(target=UnityBackTarget.all_unity),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_unity_invitation(idpk_user: int | str):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("accept_to_unity"),
        callback_data=UnityRequestDecisionCallback(
            user_idpk=int(idpk_user),
            decision=UnityRequestDecision.accept,
        ),
    )
    builder.button(
        text=await tools.get_text_button("rejected_to_unity"),
        callback_data=UnityRequestDecisionCallback(
            user_idpk=int(idpk_user),
            decision=UnityRequestDecision.reject,
        ),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_npc_unity_invitation(unity_idpk: int, owner_idpk: int):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("accept_to_unity"),
        callback_data=NpcUnityInviteDecisionCallback(
            unity_idpk=unity_idpk,
            owner_idpk=owner_idpk,
            decision=UnityRequestDecision.accept,
        ),
    )
    builder.button(
        text=await tools.get_text_button("rejected_to_unity"),
        callback_data=NpcUnityInviteDecisionCallback(
            unity_idpk=unity_idpk,
            owner_idpk=owner_idpk,
            decision=UnityRequestDecision.reject,
        ),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_menu_unity_members(
    session: AsyncSession,
    unity_idpk: int,
    row_keyboard: int = None,
    page: int = 1,
    size: int = None,
) -> InlineKeyboardBuilder:
    row_keyboard = row_keyboard or await tools.get_row_unity_members(session=session)
    size = size or await tools.get_size_unity_members(session=session)
    builder = InlineKeyboardBuilder()
    start = (page - 1) * size
    stop = start + size
    data = await tools.get_members_name_and_idpk(session=session, idpk_unity=unity_idpk)
    slice = islice(data, start, stop)
    counter = 0
    for name, idpk in slice:
        builder.button(
            text=await tools.get_text_button(
                name="pattern_name_unity_member",
                n=name,
            ),
            callback_data=UnityMemberViewCallback(member_idpk=idpk),
        )
        counter += 1
    whole_pairs = counter // row_keyboard
    remainder = counter % row_keyboard
    row = (
        [row_keyboard for _ in range(whole_pairs)] + [remainder]
        if remainder
        else [row_keyboard for _ in range(whole_pairs)]
    )

    builder.button(
        text=await tools.get_text_button("arrow_left"),
        callback_data=UnityMemberPageCallback(direction=Direction.left),
    )
    builder.button(
        text=await tools.get_text_button("arrow_right"),
        callback_data=UnityMemberPageCallback(direction=Direction.right),
    )
    builder.adjust(*row, 2, 1)

    return builder.as_markup()


async def ik_member_menu():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("delete_from_members"),
        callback_data="delete_from_members",
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=UnityMembersBackCallback(target="to_all_members"),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_back_member():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=UnityMembersBackCallback(target="to_all_members"),
    )
    return builder.as_markup()


async def ik_get_bonus(sub_on_chat: bool, sub_on_channel: bool):
    builder = InlineKeyboardBuilder()
    if not sub_on_chat:
        builder.button(
            text=await tools.get_text_button("get_bonus_chat"),
            callback_data="get_bonus_chat",
        )
    if not sub_on_channel:
        builder.button(
            text=await tools.get_text_button("get_bonus_channel"),
            callback_data="get_bonus_channel",
        )
    builder.button(
        text=await tools.get_text_button("get_bonus"), callback_data="get_bonus"
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_referrals_menu(promo_text: str):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("share_link"),
        switch_inline_query=promo_text,
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=AccountBackCallback(target=AccountBackTarget.account),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_get_money(one_piece: int, remain_pieces: int, idpk_tr: int):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button(
            "get_money", remain_pieces=remain_pieces, one_piece=one_piece
        ),
        callback_data=TransferActivateCallback(transfer_idpk=idpk_tr),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_get_money_one_piece(idpk_tr: int):
    builder = InlineKeyboardBuilder()
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("get_money_one_piece"),
        callback_data=TransferActivateCallback(transfer_idpk=idpk_tr),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_button_play(
    game_type: str, total_moves: int, remain_moves: int, autopilot: bool = False
):
    builder = InlineKeyboardBuilder()
    callback_data = "dice_autopilot" if autopilot else "dice"
    match game_type:
        case "🎯":
            builder.button(
                text=await tools.get_text_button(
                    "dart",
                    total_moves=total_moves,
                    remain_moves=remain_moves,
                ),
                callback_data=callback_data,
            )
        case "🎳":
            builder.button(
                text=await tools.get_text_button(
                    "bowling",
                    total_moves=total_moves,
                    remain_moves=remain_moves,
                ),
                callback_data=callback_data,
            )
        case "🎲":
            builder.button(
                text=await tools.get_text_button(
                    "dice",
                    total_moves=total_moves,
                    remain_moves=remain_moves,
                ),
                callback_data=callback_data,
            )
        case "⚽️":
            builder.button(
                text=await tools.get_text_button(
                    "football",
                    total_moves=total_moves,
                    remain_moves=remain_moves,
                ),
                callback_data=callback_data,
            )
        case "🏀":
            builder.button(
                text=await tools.get_text_button(
                    "basketball",
                    total_moves=total_moves,
                    remain_moves=remain_moves,
                ),
                callback_data=callback_data,
            )
    builder.adjust(1)
    return builder.as_markup()


async def ik_watch_results_game(link_on_message: str):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("watch_result"), url=link_on_message
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_choice_type_top(chosen: str):
    builder = InlineKeyboardBuilder()
    button_types = [
        ("top_income", "top_income"),
        ("top_money", "top_money"),
        ("top_animals", "top_animals"),
        ("top_referrals", "top_referrals"),
    ]

    for button_type, callback_data in button_types:
        if chosen != button_type:
            builder.button(
                text=await tools.get_text_button(button_type),
                callback_data=callback_data,
            )
    builder.adjust(1)
    return builder.as_markup()


async def ik_choice_rate_calculator(
    session: AsyncSession, current_rate: str | int = None
):
    builder = InlineKeyboardBuilder()
    emoji = "🔘"
    for rate in await tools.fetch_and_parse_str_value(
        session=session, value_name="RATES_CALCULATOR", func_to_element=str
    ):
        is_chosen = emoji if current_rate and str(current_rate) == rate else ""
        builder.button(
            text=await tools.get_text_button(
                "calculator", rate=int(rate), is_chosen=is_chosen
            ),
            callback_data=CalculatorRateCallback(rate=int(rate)),
        )
    builder.adjust(3)
    return builder.as_markup()


async def ik_im_take(idpk_message_to_support: int):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("im_take"),
        callback_data=SupportTakeCallback(
            message_idpk=idpk_message_to_support,
            action=SupportAction.take,
        ),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_confirm_or_cancel(idpk_message_to_support: int):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("confirm"),
        callback_data=SupportTakeCallback(
            message_idpk=idpk_message_to_support,
            action=SupportAction.confirm,
        ),
    )
    builder.button(
        text=await tools.get_text_button("cancel"),
        callback_data=SupportTakeCallback(
            message_idpk=idpk_message_to_support,
            action=SupportAction.cancel,
        ),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_link_on_member_support(link: str, name: str):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("link_on_member_support", name_=name),
        url=link,
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_confirm_or_change_bonus(number_attempts_item: int):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("confirm_bonus"),
        callback_data="confirm_bonus",
    )
    builder.button(
        text=await tools.get_text_button("change_bonus", nai=number_attempts_item),
        callback_data="change_bonus",
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_create_item(
    uci: int = None,
    pci: int = None,
    is_sell: bool = False,
):
    builder = InlineKeyboardBuilder()
    if is_sell:
        builder.button(
            text=await tools.get_text_button("sell_item"),
            callback_data="sell_item",
        )
    if uci:
        builder.button(text=f"Создать за {uci}$", callback_data="create_item")
    else:
        builder.button(
            text=await tools.get_text_button("create_item"),
            callback_data="create_item",
        )
    if pci:
        builder.button(text=f"Создать за {pci}🐾", callback_data="create_item_paw")
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=ForgeBackCallback(target=ForgeBackTarget.forge_menu),
    )
    row_sizes = [1] * (3 if is_sell else 2)
    if pci:
        row_sizes.append(1)
    builder.adjust(*row_sizes)
    return builder.as_markup()


async def ik_up_lvl_item():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("choice_item_for_upgrade"),
        callback_data="choice_item_for_upgrade",
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=ForgeBackCallback(target=ForgeBackTarget.forge_menu),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_menu_items_for_up(
    session: AsyncSession,
    id_user: int,
    row_keyboard: int = None,
    page: int = 1,
    size_items: int = None,
) -> InlineKeyboardBuilder:
    row_keyboard = row_keyboard or await tools.get_row_items_for_kb(session=session)
    size_items = size_items or await tools.get_size_items_for_kb(session=session)
    builder = InlineKeyboardBuilder()
    start = (page - 1) * size_items
    stop = start + size_items
    items_data = await tools.get_items_data_for_up_to_kb(
        session=session, id_user=id_user
    )
    sliced_items_name = islice(items_data, start, stop)
    count_items = 0
    for name, id_item in sliced_items_name:
        builder.button(
            text=await tools.get_text_button(name="pattern_button_name_item", n=name),
            callback_data=ForgeItemSelectCallback(
                mode=ForgePageMode.upgrade,
                item_id=id_item,
            ),
        )
        count_items += 1
    whole_pairs = count_items // row_keyboard
    remainder = count_items % row_keyboard
    row = (
        [row_keyboard for _ in range(whole_pairs)] + [remainder]
        if remainder
        else [row_keyboard for _ in range(whole_pairs)]
    )

    builder.button(
        text=await tools.get_text_button("arrow_left"),
        callback_data=ForgeItemsPageCallback(
            mode=ForgePageMode.upgrade,
            direction=Direction.left,
        ),
    )
    builder.button(
        text=await tools.get_text_button("arrow_right"),
        callback_data=ForgeItemsPageCallback(
            mode=ForgePageMode.upgrade,
            direction=Direction.right,
        ),
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=ForgeBackCallback(target=ForgeBackTarget.upgrade_info),
    )
    builder.adjust(*row, 2, 1)

    return builder.as_markup()


async def ik_upgrade_item():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("upgrade_item"),
        callback_data="upgrade_item",
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=ForgeBackCallback(target=ForgeBackTarget.choice_item),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_choice_items_to_merge():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("choice_item_to_merge"),
        callback_data="choice_item_to_merge",
    )
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=ForgeBackCallback(target=ForgeBackTarget.forge_menu),
    )
    builder.adjust(1)
    return builder.as_markup()


async def ik_menu_items_for_merge(
    session: AsyncSession,
    id_user: int,
    id_chosen_items: list[str] = None,
    row_keyboard: int = None,
    page: int = 1,
    size_items: int = None,
) -> InlineKeyboardBuilder:
    if not id_chosen_items:
        id_chosen_items = []
    row_keyboard = row_keyboard or await tools.get_row_items_for_kb(session=session)
    size_items = size_items or await tools.get_size_items_for_kb(session=session)
    builder = InlineKeyboardBuilder()
    start = (page - 1) * size_items
    stop = start + size_items
    items_data = await tools.get_items_data_for_merge_to_kb(
        session=session, id_user=id_user, id_items=id_chosen_items
    )
    sliced_items_name = islice(items_data, start, stop)
    count_items = 0
    for name, id_item in sliced_items_name:
        builder.button(
            text=await tools.get_text_button(name="pattern_button_name_item", n=name),
            callback_data=ForgeItemSelectCallback(
                mode=ForgePageMode.merge,
                item_id=id_item,
            ),
        )
        count_items += 1
    whole_pairs = count_items // row_keyboard
    remainder = count_items % row_keyboard
    row = (
        [row_keyboard for _ in range(whole_pairs)] + [remainder]
        if remainder
        else [row_keyboard for _ in range(whole_pairs)]
    )
    tail_row = [2, 1]
    builder.button(
        text=await tools.get_text_button("arrow_left"),
        callback_data=ForgeItemsPageCallback(
            mode=ForgePageMode.merge,
            direction=Direction.left,
        ),
    )
    builder.button(
        text=await tools.get_text_button("arrow_right"),
        callback_data=ForgeItemsPageCallback(
            mode=ForgePageMode.merge,
            direction=Direction.right,
        ),
    )
    if len(id_chosen_items) == 2:
        builder.button(
            text=await tools.get_text_button("merge_items"),
            callback_data="merge_items",
        )
        tail_row.append(1)
    builder.button(
        text=await tools.get_text_button("back"),
        callback_data=ForgeBackCallback(target=ForgeBackTarget.merge_info),
    )
    builder.adjust(*row, *tail_row)

    return builder.as_markup()


async def ik_yes_or_not_sell_item():
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("sell_item_yes"),
        callback_data="sell_item_yes",
    )
    builder.button(
        text=await tools.get_text_button("sell_item_no"),
        callback_data="sell_item_no",
    )
    builder.adjust(2)
    return builder.as_markup()
