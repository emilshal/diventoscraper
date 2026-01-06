from __future__ import annotations

from typing import List


def classify(text: str) -> List[int]:
    """
    Lightweight fallback classifier.

    The original project used an ML model to assign category flags.
    For current workflows, returning an all-zero vector is acceptable; callers
    will fall back to sensible defaults.
    """
    _ = text
    return [0] * 11

