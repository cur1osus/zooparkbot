from aiogram import Router


def setup_message_routers() -> Router:
    from . import (
        admin_panel,
        account,
        any_unknown_message,
        aviaries,
        bank,
        bonus,
        command_add_ban_word,
        command_ban,
        command_calculator,
        command_donate,
        command_faq,
        command_history,
        command_mailing,
        command_photo_view,
        command_support,
        command_usd,
        errors,
        forge_items,
        im_take,
        inline_create_game,
        inline_rate,
        inline_send_money,
        inline_send_ref_link,
        play_game,
        random_merchant,
        rarity_shop,
        referrals,
        start,
        top,
        unity,
        unity_project,
        unity_level,
        unity_members,
        unity_top,
        zoomarket,
    )

    router = Router()
    router.include_router(start.router)
    router.include_router(admin_panel.router)
    router.include_router(command_usd.router)
    router.include_router(command_support.router)
    router.include_router(command_calculator.router)
    router.include_router(command_faq.router)
    router.include_router(command_ban.router)
    router.include_router(command_donate.router)
    router.include_router(command_photo_view.router)
    router.include_router(command_history.router)
    router.include_router(command_mailing.router)
    router.include_router(command_add_ban_word.router)
    router.include_router(referrals.router)
    router.include_router(zoomarket.router)
    router.include_router(unity.router)
    router.include_router(unity_project.router)
    router.include_router(unity_level.router)
    router.include_router(unity_members.router)
    router.include_router(unity_top.router)
    router.include_router(bank.router)
    router.include_router(top.router)
    router.include_router(bonus.router)
    router.include_router(inline_send_money.router)
    router.include_router(inline_create_game.router)
    router.include_router(inline_rate.router)
    router.include_router(play_game.router)
    router.include_router(inline_send_ref_link.router)
    router.include_router(forge_items.router)
    router.include_router(account.router)
    router.include_router(random_merchant.router)
    router.include_router(rarity_shop.router)
    # router.include_router(workshop_items.router)
    router.include_router(aviaries.router)
    router.include_router(im_take.router)

    router.include_router(any_unknown_message.router)

    # router.include_router(errors.router)

    return router
