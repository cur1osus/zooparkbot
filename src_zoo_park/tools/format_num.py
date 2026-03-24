from abc import ABC, abstractmethod
from decimal import Decimal


class NumberFormatter(ABC):
    """Абстрактный класс для форматирования чисел."""

    @abstractmethod
    def format_number(self, number: int) -> str:
        """Метод для форматирования числа."""
        pass


class NonillionFormatter(NumberFormatter):
    threshold = 10 ** 30

    def format_number(self, number: int) -> str:
        return f"{Decimal(number) / Decimal(10 ** 30):.1f}No"


class OctillionFormatter(NumberFormatter):
    threshold = 10 ** 27

    def format_number(self, number: int) -> str:
        return f"{Decimal(number) / Decimal(10 ** 27):.1f}Oc"


class SeptillionFormatter(NumberFormatter):
    threshold = 10 ** 24

    def format_number(self, number: int) -> str:
        return f"{Decimal(number) / Decimal(10 ** 24):.1f}Sp"


class SextillionFormatter(NumberFormatter):
    """Форматировщик для секстиллионов."""

    threshold = 1_000_000_000_000_000_000_000

    def format_number(self, number: int) -> str:
        return f"{Decimal(number) / Decimal(1_000_000_000_000_000_000_000):.1f}Sx"


class QuintillionFormatter(NumberFormatter):

    threshold = 1_000_000_000_000_000_000

    def format_number(self, number: int) -> str:
        return f"{number / 1_000_000_000_000_000_000:,.1f}Qn"


class QuadrillionFormatter(NumberFormatter):

    threshold = 1_000_000_000_000_000

    def format_number(self, number: int) -> str:
        return f"{number / 1_000_000_000_000_000:,.1f}Qd"


class TrillionFormatter(NumberFormatter):
    """Форматировщик для триллионов."""

    threshold = 1_000_000_000_000

    def format_number(self, number: int) -> str:
        return f"{number / 1_000_000_000_000:,.1f}T"


class BillionFormatter(NumberFormatter):
    """Форматировщик для миллиардов."""

    threshold = 1_000_000_000

    def format_number(self, number: int) -> str:
        return f"{number / 1_000_000_000:,.1f}B"


class MillionFormatter(NumberFormatter):
    """Форматировщик для миллионов."""

    threshold = 1_000_000

    def format_number(self, number: int) -> str:
        return f"{number / 1_000_000:,.1f}M"


class ThousandFormatter(NumberFormatter):
    """Форматировщик для тысяч."""

    threshold = 1_000

    def format_number(self, number: int) -> str:
        return f"{number / 1_000:,.1f}k"


class DefaultFormatter(NumberFormatter):
    """Форматировщик по умолчанию для чисел меньше 1000."""

    threshold = 0

    def format_number(self, number: int) -> str:
        return f"{number:,.0f}"


class LargeNumberFormatter:
    """Класс для выбора подходящего форматировщика."""

    def __init__(self):
        self.formatters = [
            NonillionFormatter(),
            OctillionFormatter(),
            SeptillionFormatter(),
            SextillionFormatter(),
            QuintillionFormatter(),
            QuadrillionFormatter(),
            TrillionFormatter(),
            BillionFormatter(),
            MillionFormatter(),
            ThousandFormatter(),
            DefaultFormatter(),
        ]

    def format_large_number(self, number: int | float, **kw) -> str:
        """Выбор форматировщика и форматирование числа."""
        # Fallback to scientific notation for astronomically large numbers
        if number >= 10 ** 33:
            d = Decimal(number)
            exp = len(str(int(d))) - 1
            mantissa = d / Decimal(10) ** exp
            return f"{mantissa:.2f}E+{exp}"
        for formatter in self.formatters:
            if number < formatter.threshold:
                continue
            return formatter.format_number(number)


# Примеры использования
formatter = LargeNumberFormatter()
