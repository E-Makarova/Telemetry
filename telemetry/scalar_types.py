from __future__ import annotations

import math
from abc import ABC, abstractmethod
from typing import Any


class ScalarType(ABC):
    name: str

    @abstractmethod
    def validate(self, value: Any) -> int | float | bool:
        raise NotImplementedError


class IntegerType(ScalarType):
    name = "integer"

    def validate(self, value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("expected integer")
        return value


class FloatType(ScalarType):
    name = "float"

    def validate(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("expected finite float")
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("expected finite float")
        return result


class BooleanType(ScalarType):
    name = "boolean"

    def validate(self, value: Any) -> bool:
        if not isinstance(value, bool):
            raise ValueError("expected boolean")
        return value


class ScalarTypeRegistry:
    def __init__(self) -> None:
        self._types: dict[str, ScalarType] = {}

    def register(self, scalar_type: ScalarType) -> None:
        self._types[scalar_type.name] = scalar_type

    def validate(self, type_name: str, value: Any) -> int | float | bool:
        try:
            scalar_type = self._types[type_name]
        except KeyError as exc:
            raise ValueError(f"unknown scalar type: {type_name}") from exc
        return scalar_type.validate(value)

    def names(self) -> set[str]:
        return set(self._types)


def default_scalar_registry() -> ScalarTypeRegistry:
    registry = ScalarTypeRegistry()
    registry.register(IntegerType())
    registry.register(FloatType())
    registry.register(BooleanType())
    return registry

