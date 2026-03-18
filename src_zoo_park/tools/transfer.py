from db.structured_state import add_transfer_claim, has_transfer_claim
from sqlalchemy.ext.asyncio import AsyncSession


async def in_used(session: AsyncSession, idpk_tr: int, idpk_user: int) -> bool:
    return await has_transfer_claim(
        session=session,
        transfer_idpk=idpk_tr,
        user_idpk=idpk_user,
    )


async def add_user_to_used(session: AsyncSession, idpk_tr: int, idpk_user: int):
    await add_transfer_claim(
        session=session,
        transfer_idpk=idpk_tr,
        user_idpk=idpk_user,
    )
    await session.commit()
