from enum import Enum

from aiogram.filters.callback_data import CallbackData


class Direction(str, Enum):
    left = "left"
    right = "right"


class RandomMerchantOfferCallback(CallbackData, prefix="rmof"):
    offer: int


class RandomMerchantAnimalCallback(CallbackData, prefix="rman"):
    animal: str


class RandomMerchantQuantityCallback(CallbackData, prefix="rmqt"):
    quantity: int


class RandomMerchantBackTarget(str, Enum):
    all_offers = "to_all_offers"
    choice_animal = "to_choice_animal"


class RandomMerchantBackCallback(CallbackData, prefix="rmbk"):
    target: RandomMerchantBackTarget


class RarityShopAnimalCallback(CallbackData, prefix="rsan"):
    animal: str


class RarityShopRarityCallback(CallbackData, prefix="rsrt"):
    rarity: str


class RarityShopSwitchCallback(CallbackData, prefix="rssw"):
    direction: Direction


class RarityShopQuantityCallback(CallbackData, prefix="rsqt"):
    quantity: int


class RarityShopBackTarget(str, Enum):
    choice_animal = "to_choice_animal"
    choice_rarity = "to_choice_rarity"


class RarityShopBackCallback(CallbackData, prefix="rsbk"):
    target: RarityShopBackTarget


class AccountItemPageCallback(CallbackData, prefix="acpg"):
    direction: Direction


class AccountItemViewCallback(CallbackData, prefix="acit"):
    item_id: str


class AccountBackTarget(str, Enum):
    account = "to_account"
    items = "to_items"


class AccountBackCallback(CallbackData, prefix="acbk"):
    target: AccountBackTarget


class UnityPageCallback(CallbackData, prefix="unpg"):
    direction: Direction


class UnityViewCallback(CallbackData, prefix="unvw"):
    owner_idpk: int


class UnityBackTarget(str, Enum):
    menu = "to_menu_unity"
    all_unity = "to_all_unity"


class UnityBackCallback(CallbackData, prefix="unbk"):
    target: UnityBackTarget


class UnityRequestDecision(str, Enum):
    accept = "accept"
    reject = "reject"


class UnityRequestDecisionCallback(CallbackData, prefix="unrq"):
    user_idpk: int
    decision: UnityRequestDecision


class NpcUnityInviteDecisionCallback(CallbackData, prefix="nuni"):
    unity_idpk: int
    owner_idpk: int
    decision: UnityRequestDecision


class UnityMemberPageCallback(CallbackData, prefix="umpg"):
    direction: Direction


class UnityMemberViewCallback(CallbackData, prefix="umvw"):
    member_idpk: int


class UnityMembersBackCallback(CallbackData, prefix="umbk"):
    target: str


class AviaryChoiceCallback(CallbackData, prefix="avch"):
    aviary: str


class AviaryQuantityCallback(CallbackData, prefix="avqt"):
    quantity: int


class AviaryBackCallback(CallbackData, prefix="avbk"):
    target: str


class WorkshopBackCallback(CallbackData, prefix="wibk"):
    target: str


class WorkshopItemChoiceCallback(CallbackData, prefix="wich"):
    item_code: str


class ForgeBackTarget(str, Enum):
    forge_menu = "to_forge_items_menu"
    upgrade_info = "to_up_lvl_item_info"
    choice_item = "to_choice_item"
    merge_info = "to_merge_items_info"


class ForgeBackCallback(CallbackData, prefix="fibk"):
    target: ForgeBackTarget


class ForgePageMode(str, Enum):
    upgrade = "up"
    merge = "merge"


class ForgeItemsPageCallback(CallbackData, prefix="fipg"):
    mode: ForgePageMode
    direction: Direction


class ForgeItemSelectCallback(CallbackData, prefix="fisl"):
    mode: ForgePageMode
    item_id: str


class SupportAction(str, Enum):
    take = "take"
    confirm = "confirm"
    cancel = "cancel"


class SupportTakeCallback(CallbackData, prefix="sptk"):
    message_idpk: int
    action: SupportAction


class CalculatorRateCallback(CallbackData, prefix="calc"):
    rate: int


class TransferActivateCallback(CallbackData, prefix="trac"):
    transfer_idpk: int


class InlineRateUpdateCallback(CallbackData, prefix="inrt"):
    token: str
