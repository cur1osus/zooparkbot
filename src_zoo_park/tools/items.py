import json
import math
import random
from abc import ABC, abstractmethod
from collections import defaultdict

from db import Item
from game_variables import (
    colors_rarities_item,
    prop_quantity_by_rarity,
    rarities,
    rarity_by_prop_quantity,
    num_for_sort_rarity,
)
from init_db import _sessionmaker_for_func
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

import tools


CREATE_ITEM_PAW_PRICE = 350


async def get_items_data_to_kb(
    session: AsyncSession,
    id_user: int,
) -> list[tuple[str, str, str]]:
    EMOJI_FOR_ACTIVATE_ITEM = await tools.get_value(
        session=session,
        value_name="EMOJI_FOR_ACTIVATE_ITEM",
        value_type="str",
    )
    items = await session.scalars(select(Item).where(and_(Item.id_user == id_user)))
    items = items.all()
    items = sorted(items, key=sorted_by_rarity, reverse=True)
    data = []
    for item in items:
        name_with_emoji = await tools.get_text_button(
            "pattern_for_item_button_attrs",
            lvl=item.lvl,
            name_with_emoji=item.name_with_emoji,
            emoji_rarity=colors_rarities_item[item.rarity],
        )
        name_with_emoji += f" {EMOJI_FOR_ACTIVATE_ITEM}" if item.is_active else ""
        if item.is_active:
            data.insert(0, (name_with_emoji, item.id_item))
        else:
            data.append((name_with_emoji, item.id_item))
    return data


async def get_items_data_for_up_to_kb(
    session: AsyncSession,
    id_user: int,
) -> list[tuple[str, str, str]]:
    EMOJI_FOR_ACTIVATE_ITEM = await tools.get_value(
        session=session,
        value_name="EMOJI_FOR_ACTIVATE_ITEM",
        value_type="str",
    )
    items = await session.scalars(select(Item).where(and_(Item.id_user == id_user)))
    items = items.all()
    items = sorted(items, key=sorted_by_rarity, reverse=True)
    data = []
    for item in items:
        name_with_emoji = await tools.get_text_button(
            "pattern_for_item_button_attrs",
            lvl=item.lvl,
            name_with_emoji=item.name_with_emoji,
            emoji_rarity=colors_rarities_item[item.rarity],
        )
        name_with_emoji += f" {EMOJI_FOR_ACTIVATE_ITEM}" if item.is_active else ""
        if item.is_active:
            data.insert(
                0,
                (
                    name_with_emoji,
                    item.id_item,
                ),
            )
        else:
            data.append((name_with_emoji, item.id_item))
    return data


def sorted_by_rarity(item: Item):
    return num_for_sort_rarity[item.rarity]


async def get_items_data_for_merge_to_kb(
    session: AsyncSession, id_user: int, id_items: list[str]
) -> list[tuple[str, str, str]]:
    EMOJI_FOR_ACTIVATE_ITEM = await tools.get_value(
        session=session,
        value_name="EMOJI_FOR_ACTIVATE_ITEM",
        value_type="str",
    )
    EMOJI_FOR_CHOSEN_ITEM = await tools.get_value(
        session=session,
        value_name="EMOJI_FOR_CHOSEN_ITEM",
        value_type="str",
    )
    items = await session.scalars(select(Item).where(and_(Item.id_user == id_user)))
    items = items.all()
    items = sorted(items, key=sorted_by_rarity, reverse=True)
    data = []
    for item in items:
        name_with_emoji = await tools.get_text_button(
            "pattern_for_item_button_attrs",
            lvl=item.lvl,
            name_with_emoji=item.name_with_emoji,
            emoji_rarity=colors_rarities_item[item.rarity],
        )
        name_with_emoji += f" {EMOJI_FOR_ACTIVATE_ITEM}" if item.is_active else ""
        name_with_emoji += (
            f" {EMOJI_FOR_CHOSEN_ITEM}" if item.id_item in id_items else ""
        )
        if item.is_active:
            data.insert(
                0,
                (
                    name_with_emoji,
                    item.id_item,
                ),
            )
        else:
            data.append((name_with_emoji, item.id_item))
    return data


async def get_row_items_for_kb(session: AsyncSession):
    row_items = await tools.get_value(session=session, value_name="ROW_ITEMS_FOR_KB")
    return row_items


async def get_size_items_for_kb(session: AsyncSession):
    size_items = await tools.get_value(session=session, value_name="SIZE_ITEMS_FOR_KB")
    return size_items


async def count_page_items(session: AsyncSession, amount_items: int) -> int:
    size_items = await get_size_items_for_kb(session=session)
    return math.ceil(amount_items / size_items)


async def get_property_probability(name_property: str):
    name = f"{name_property.upper()}_PROBABILITY"
    async with _sessionmaker_for_func() as session:
        r = await tools.get_value(session=session, value_name=name, value_type="str")
        return float(r)


async def get_borders_property(name_property: str):
    name = f"{name_property.upper()}_BORDERS"
    async with _sessionmaker_for_func() as session:
        r = await tools.get_value(session=session, value_name=name, value_type="str")
        return [int(i.strip()) for i in r.split(",")]


async def get_animal_probability() -> list[float]:
    name = "ANIMAL_PROBABILITY_FOR_ITEMS"
    async with _sessionmaker_for_func() as session:
        r = await tools.get_value(session=session, value_name=name, value_type="str")
        return [float(i.strip()) for i in r.split(", ")]


async def get_rarity_animal_probability() -> list[float]:
    name = "RARITY_ANIMAL_PROBABILITY_FOR_ITEMS"
    async with _sessionmaker_for_func() as session:
        r = await tools.get_value(session=session, value_name=name, value_type="str")
        return [float(i.strip()) for i in r.split(", ")]


def gen_name_and_emoji_item(item_props: str | dict) -> tuple[str, str]:
    if isinstance(item_props, str):
        item_props = json.loads(item_props)
    prefixes = {
        "animal1": "Зайце",
        "animal3": "Крыло",
        "animal4": "Кито",
        "animal7": "Копыто",
        "animal8": "Медведе",
        "animal9": "Пёсо",
        "animal10": "Кошка",
    }
    bases = {
        1: "Камень",
        2: "Кристалл",
        3: "Талисман",
        4: "Амулет",
        5: "Ключ",
        6: "Компас",
        7: "Фонарь",
        8: "Тотем",
    }
    attributes = {
        IncomeProperty().name: "Дохода",
        ExchangeBankProperty().name: "Обмена",
        AviariesSaleProperty().name: "Вольера",
        AnimalIncomeProperty().name: "Жизни",
        ExtraMoves().name: "Интеллекта",
        LastChance().name: "Ловкости",
        BonusChanger().name: "Удачи",
        AnimalSaleProperty().name: "Скидки",
    }

    emojis = {
        "Дохода": "📈",
        "Обмена": "🔄",
        "Вольера": "⛓",
        "Жизни": "🐥",
        "Интеллекта": "🧠",
        "Ловкости": "⚡️",
        "Удачи": "🍀",
        "Скидки": "📉",
    }
    prefix = None
    # Генерация названия предмета
    for prop in item_props.keys():
        if ":" not in prop:
            continue
        animal = prop.split(":")[0]
        animal = animal.split("_")[0]
        prefix = prefixes.get(animal, "Живой")
    if not prefix:
        prefix = "Серый"
    count_props = len(item_props)
    base = bases[count_props]
    name_max_value_prop = max(item_props.items(), key=lambda x: x[1])[0]
    name_max_value_prop = (
        name_max_value_prop.split(":")[1]
        if ":" in name_max_value_prop
        else name_max_value_prop
    )
    attribute = attributes[name_max_value_prop]
    emoji = emojis[attribute]

    return f"{prefix}-{base} {attribute}", emoji


# Абстрактный базовый класс для свойства
class Property(ABC):
    name = "base_property"

    async def get_weight(self, rarity: str):
        property_probability = await get_property_probability(
            name_property=f"{self.name}_{rarity}"
        )
        return property_probability

    @abstractmethod
    async def generate(self, rarity: str):
        pass


# Пример конкретного свойства: Сила
class IncomeProperty(Property):
    name = "general_income"

    async def generate(self, rarity: str):
        name_property = f"{self.name}_{rarity}"
        border_min, border_max = await get_borders_property(name_property=name_property)
        value = random.randint(border_min, border_max)
        return [self.name, value]


# Пример конкретного свойства: Здоровье
class ExchangeBankProperty(Property):
    name = "exchange_bank"

    async def generate(self, rarity: str):
        name_property = f"{self.name}_{rarity}"
        border_min, border_max = await get_borders_property(name_property=name_property)
        value = random.randint(border_min, border_max)
        return [self.name, value]


# Пример конкретного свойства: Скорость
class AviariesSaleProperty(Property):
    name = "aviaries_sale"

    async def generate(self, rarity: str):
        name_property = f"{self.name}_{rarity}"
        border_min, border_max = await get_borders_property(name_property=name_property)
        value = random.randint(border_min, border_max)
        return [self.name, value]


class AnimalIncomeProperty(Property):
    name = "animal_income"

    @classmethod
    async def get_random_animal(cls, quantity_animal: int = 10) -> str:
        weights_animal = await get_animal_probability()
        weights_rarity = await get_rarity_animal_probability()
        rarity = random.choices(rarities, weights=weights_rarity)[0]
        num_animal = random.choices(
            list(range(1, quantity_animal + 1)), weights=weights_animal
        )[0]
        return f"animal{num_animal}{rarity}"

    async def generate(self, rarity: str):
        name_property = f"{self.name}_{rarity}"
        border_min, border_max = await get_borders_property(name_property=name_property)
        value = random.randint(border_min, border_max)
        animal = await self.get_random_animal()
        name = f"{animal}:{self.name}"
        return [name, value]


class AnimalSaleProperty(Property):
    name = "animal_sale"

    async def generate(self, rarity: str):
        name_property = f"{self.name}_{rarity}"
        border_min, border_max = await get_borders_property(name_property=name_property)
        value = random.randint(border_min, border_max)
        animal = await AnimalIncomeProperty.get_random_animal()
        name = f"{animal}:{self.name}"
        return [name, value]


class BonusChanger(Property):
    name = "bonus_changer"

    async def generate(self, rarity: str):
        name_property = f"{self.name}_{rarity}"
        border_min, border_max = await get_borders_property(name_property=name_property)
        value = random.randint(border_min, border_max)
        return [self.name, value]


class LastChance(Property):
    name = "last_chance"

    async def generate(self, rarity: str):
        name_property = f"{self.name}_{rarity}"
        border_min, border_max = await get_borders_property(name_property=name_property)
        value = random.randint(border_min, border_max)
        return [self.name, value]


class ExtraMoves(Property):
    name = "extra_moves"

    async def generate(self, rarity: str):
        name_property = f"{self.name}_{rarity}"
        border_min, border_max = await get_borders_property(name_property=name_property)
        value = random.randint(border_min, border_max)
        return [self.name, value]


# Класс генерации свойств, который использует конкретные реализации свойств
class PropertyGenerator:
    def __init__(self, properties, rarity):
        self.properties = properties
        self.rarity = rarity

    async def get_weight_properties(self):
        return [await prop.get_weight(self.rarity) for prop in self.properties]

    async def generate_properties(self):
        weight_properties = await self.get_weight_properties()
        props = {}
        counter_prop = 0
        while counter_prop < prop_quantity_by_rarity[self.rarity]:
            obj_prop: Property = random.choices(
                self.properties, weights=weight_properties
            )[0]
            prop: dict = await obj_prop.generate(rarity=self.rarity)
            k, v = prop
            if k in props:
                props[k] += v
            else:
                props[k] = v
            counter_prop += 1
        return props


async def gen_rarity_item(session: AsyncSession):
    weights_rarity = await tools.fetch_and_parse_str_value(
        session=session, value_name="WEIGHT_RARITIES_ITEM", func_to_element=float
    )
    return random.choices(list(prop_quantity_by_rarity.keys()), weights=weights_rarity)[
        0
    ]


async def create_item(session: AsyncSession):
    rarity = await gen_rarity_item(session=session)
    properties = [
        IncomeProperty(),
        ExchangeBankProperty(),
        AviariesSaleProperty(),
        AnimalIncomeProperty(),
        ExtraMoves(),
        LastChance(),
        BonusChanger(),
        AnimalSaleProperty(),
    ]
    property_generator = PropertyGenerator(properties=properties, rarity=rarity)
    item_props = await property_generator.generate_properties()
    item_name, emoji = gen_name_and_emoji_item(item_props=item_props)
    key = tools.gen_key(12)
    item_info = {"name": item_name, "emoji": emoji, "rarity": rarity, "key": key}
    return item_info, item_props


async def add_item_to_db(
    session: AsyncSession, item_info: dict, item_props: dict, id_user: int
):
    item = Item(
        id_item=item_info["key"],
        id_user=id_user,
        emoji=item_info["emoji"],
        name=item_info["name"],
        properties=json.dumps(item_props),
        rarity=item_info["rarity"],
    )
    session.add(item)
    await session.commit()


async def able_to_enhance(session: AsyncSession, current_item_lvl: int):
    percent = await calculate_percent_to_enhance(
        session=session, current_item_lvl=current_item_lvl
    )
    percent /= 100
    return random.choices([True, False], [percent, 1 - percent])[0]


async def calculate_percent_to_enhance(session: AsyncSession, current_item_lvl: int):
    PERCENTAGE_DECREASE_ENHANCE_BY_LVL = await tools.get_value(
        session=session, value_name="PERCENTAGE_DECREASE_ENHANCE_BY_LVL"
    )
    percent = 100 - (PERCENTAGE_DECREASE_ENHANCE_BY_LVL * current_item_lvl)
    return percent


async def random_up_property_item(session: AsyncSession, item_properties: dict | str):
    if isinstance(item_properties, str):
        item_properties = json.loads(item_properties)
    property = random.choice(list(item_properties.keys()))
    parameter = await tools.get_value(
        session=session, value_name=f"{property.upper()}_VALUE_TO_ADD"
    )
    item_properties[property] += parameter
    item_properties = json.dumps(item_properties)
    return item_properties, property, parameter


def name_property_processing(name_prop: str):
    if ":" in name_prop:
        name_prop = name_prop.split(":")[1]
    return name_prop


async def synchronize_info_about_items(items: list[Item]):
    info_about_items = {}
    properties_to_limit = [
        AviariesSaleProperty.name,
        ExchangeBankProperty.name,
        AnimalSaleProperty.name,
    ]
    limit = 80
    for item in items:
        properties: dict = json.loads(item.properties)
        for prop, value in properties.items():
            if prop in info_about_items:
                info_about_items[prop] += value
            else:
                info_about_items[prop] = value
            clean_prop = name_property_processing(prop)
            if clean_prop in properties_to_limit and info_about_items[prop] > limit:
                info_about_items[prop] = limit
    return json.dumps(info_about_items)


async def update_prop_iai(
    info_about_items: str | dict, prop: str, value: int
):  # iai - info about items
    properties_to_limit = [AviariesSaleProperty.name, ExchangeBankProperty.name]
    limit = 80
    if isinstance(info_about_items, str):
        info_about_items = json.loads(info_about_items)
    info_about_items[prop] += value
    if prop in properties_to_limit and info_about_items[prop] > limit:
        info_about_items[prop] = limit
    return json.dumps(info_about_items)


async def merge_items(session: AsyncSession, id_item_1: str, id_item_2: str):
    item_1 = await session.scalar(select(Item).where(Item.id_item == id_item_1))
    item_2 = await session.scalar(select(Item).where(Item.id_item == id_item_2))
    prop_1: dict = json.loads(item_1.properties)
    prop_2: dict = json.loads(item_2.properties)
    count_props = len(prop_1) + len(prop_2)
    weight_true, weight_false = await calculate_weight_merge(
        session=session, count_props=count_props
    )
    max_len = max(len(prop_1), len(prop_2))
    new_props = defaultdict(int)
    for _ in range(max_len):
        if random.choices([True, False], [weight_true, weight_false])[0]:
            for prop in [prop_1, prop_2]:
                chosen_prop, value = choice_prop(prop)
                new_props[chosen_prop] += value
                if chosen_prop != "plug":
                    del prop[chosen_prop]
        else:
            take_from = random.choice([prop_1, prop_2])
            chosen_prop, value = choice_prop(take_from)
            new_props[chosen_prop] += value
            if chosen_prop != "plug":
                del take_from[chosen_prop]
    if "plug" in new_props:
        del new_props["plug"]
    item_1.id_user = 0
    item_2.id_user = 0
    name, emoji = gen_name_and_emoji_item(item_props=new_props)
    new_item = Item(
        id_item=tools.gen_key(12),
        id_user=0,
        emoji=emoji,
        name=name,
        properties=json.dumps(new_props),
        rarity=get_rarity_by_amount_props(props=new_props),
    )
    return new_item


async def calculate_weight_merge(session: AsyncSession, count_props: int):
    PERCENT_MERGE_BY_PROP = await tools.get_value(
        session=session, value_name="PERCENT_MERGE_BY_PROP"
    )
    weight_true = 100 - (PERCENT_MERGE_BY_PROP * count_props)
    weight_false = 100 - weight_true
    return weight_true, weight_false


def choice_prop(prop: dict):
    return random.choice(list(prop.items())) if prop else ("plug", 0)


def get_rarity_by_amount_props(props: dict):
    if len(props) > 4:
        return rarity_by_prop_quantity[5]
    return rarity_by_prop_quantity[len(props)]


def get_value_prop_from_iai(info_about_items: str | dict, name_prop: str):
    if isinstance(info_about_items, str):
        info_about_items = json.loads(info_about_items)
    return info_about_items.get(name_prop, None)


async def gen_price_to_create_item(session: AsyncSession, id_user: int):
    USD_TO_CREATE_ITEM = await tools.get_value(
        session=session, value_name="USD_TO_CREATE_ITEM"
    )
    PERCENT_EXTRA_CHARGE_BY_ITEM = await tools.get_value(
        session=session, value_name="PERCENT_EXTRA_CHARGE_BY_ITEM"
    )
    count_items = await session.scalar(
        select(func.count(Item.id_item)).where(Item.id_user == id_user)
    )
    percent = 1 + PERCENT_EXTRA_CHARGE_BY_ITEM / 100
    compounded_price = USD_TO_CREATE_ITEM * (percent**count_items)
    return int(compounded_price)
