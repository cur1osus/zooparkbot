from db import User
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession


async def get_referrals_count_map(
    session: AsyncSession, idpk_users: list[int]
) -> dict[int, int]:
    if not idpk_users:
        return {}

    rows = await session.execute(
        select(User.id_referrer, func.count(User.idpk))
        .where(User.id_referrer.in_(idpk_users))
        .group_by(User.id_referrer)
    )
    return {
        id_referrer: count
        for id_referrer, count in rows.all()
        if id_referrer is not None
    }


async def get_referrals(session: AsyncSession, user: User):
    return (await get_referrals_count_map(session=session, idpk_users=[user.idpk])).get(
        user.idpk, 0
    )


async def get_verify_referrals(session: AsyncSession, user: User):
    return (
        await session.scalar(
            select(func.count())
            .select_from(User)
            .where(
                and_(
                    User.id_referrer == user.idpk,
                    User.referral_verification == True,  # noqa: E712
                )
            )
        )
        or 0
    )
