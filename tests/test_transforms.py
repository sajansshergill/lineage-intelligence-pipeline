from __future__ import annotations

from etl.transforms.registry import TRANSFORM_REGISTRY, _load_all_transforms


def test_registry_loads_core_transforms():
    _load_all_transforms()
    assert {"NullFill", "TypeCast", "Dedup", "FxConvert", "WriteStarSchema"} <= set(TRANSFORM_REGISTRY)
