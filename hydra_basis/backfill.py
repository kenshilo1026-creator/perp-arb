from __future__ import annotations

from typing import Sequence, TypeVar


T = TypeVar("T")
LORIS_BATCHED_VENUES = {"variational"}


def chunk_sequence(items: Sequence[T], *, chunk_size: int) -> list[list[T]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [list(items[index:index + chunk_size]) for index in range(0, len(items), chunk_size)]


def split_loris_batched_keys(
    keys: Sequence[tuple[str, str]],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    immediate: list[tuple[str, str]] = []
    batched: list[tuple[str, str]] = []
    for key in keys:
        if key[0] in LORIS_BATCHED_VENUES:
            batched.append(key)
        else:
            immediate.append(key)
    return immediate, batched
