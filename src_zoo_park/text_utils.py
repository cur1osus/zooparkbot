import hashlib
import re
from datetime import datetime
from textwrap import shorten, wrap
from typing import Any, Iterable


def compact_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _wrap_first_chunk(text: str, width: int) -> str:
    chunks = wrap(
        text,
        width=max(1, int(width)),
        max_lines=1,
        break_long_words=True,
        break_on_hyphens=False,
    )
    return chunks[0] if chunks else ""


def preview_text(
    value: Any,
    *,
    max_chars: int,
    placeholder: str = "...",
) -> str:
    text = compact_text(value)
    if not text:
        return ""
    if len(text) <= max_chars:
        return text

    shortened = shorten(text, width=max_chars, placeholder=placeholder)
    if shortened and shortened != placeholder:
        return shortened

    width = max(1, int(max_chars) - len(placeholder))
    chunk = _wrap_first_chunk(text, width)
    if not chunk:
        return _wrap_first_chunk(placeholder or ".", max_chars)
    return f"{chunk}{placeholder}" if len(text) > len(chunk) else chunk


def semantic_preview(
    value: Any,
    *,
    max_segments: int = 2,
    max_words: int = 24,
    max_chars: int = 180,
    placeholder: str = "",
) -> str:
    text = compact_text(value)
    if not text:
        return ""

    segments = [
        segment.strip(" ,;:-")
        for segment in re.split(r"(?<=[.!?])\s+|\s+\|\s+|\s+->\s+|;\s+", text)
        if segment.strip()
    ]
    if not segments:
        segments = [text]

    selected: list[str] = []
    words_used = 0
    chars_used = 0
    for segment in segments:
        segment_words = len(segment.split())
        projected_chars = chars_used + len(segment) + (2 if selected else 0)
        if selected and (
            len(selected) >= max_segments
            or words_used + segment_words > max_words
            or projected_chars > max_chars
        ):
            break
        if not selected and (segment_words > max_words or len(segment) > max_chars):
            return preview_text(segment, max_chars=max_chars, placeholder=placeholder)
        selected.append(segment)
        words_used += segment_words
        chars_used = projected_chars
        if len(selected) >= max_segments:
            break

    if selected:
        candidate = "; ".join(selected)
        if len(candidate) <= max_chars:
            return candidate
    return preview_text(text, max_chars=max_chars, placeholder=placeholder)


def fit_db_field(value: Any, *, max_len: int, default: str = "") -> str:
    text = compact_text(value) or default
    if len(text) <= max_len:
        return text

    digest = _wrap_first_chunk(hashlib.sha1(text.encode("utf-8")).hexdigest(), 8)
    if max_len <= len(digest):
        return _wrap_first_chunk(digest, max_len)

    prefix_width = max(1, max_len - len(digest) - 1)
    prefix = preview_text(text, max_chars=prefix_width, placeholder="")
    prefix = prefix.rstrip(" -_:;,.#")
    if prefix:
        return f"{prefix}-{digest}"
    return digest


def normalize_choice(value: Any, *, allowed: Iterable[str], default: str) -> str:
    allowed_values = {str(item).strip() for item in allowed if str(item).strip()}
    candidate = compact_text(value).lower().replace(" ", "_")
    return candidate if candidate in allowed_values else default


def preview_error(value: Any, *, max_chars: int = 240) -> str:
    return preview_text(value, max_chars=max_chars, placeholder="...")


def preview_with_prefix(
    prefix: Any,
    detail: Any,
    *,
    max_chars: int,
    separator: str = " | ",
) -> str:
    prefix_text = compact_text(prefix)
    detail_text = compact_text(detail)
    if not prefix_text:
        return preview_text(detail_text, max_chars=max_chars, placeholder="...")
    if not detail_text:
        return preview_text(prefix_text, max_chars=max_chars, placeholder="...")

    remaining = max_chars - len(prefix_text) - len(separator)
    if remaining <= 0:
        return preview_text(prefix_text, max_chars=max_chars, placeholder="...")

    return (
        f"{prefix_text}{separator}"
        f"{semantic_preview(detail_text, max_chars=remaining, max_words=max(6, remaining // 5), placeholder='...')}"
    )


def format_iso_datetime_short(value: Any, default: str = "-") -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")

    text = compact_text(value)
    if not text:
        return default

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return default
    return parsed.strftime("%Y-%m-%d %H:%M")


def extract_iso_day(value: Any, default: str = "unknown") -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()

    text = compact_text(value)
    if not text:
        return default

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return default
    return parsed.date().isoformat()
