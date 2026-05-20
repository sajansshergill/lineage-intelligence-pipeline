"""
etl/transforms/registry.py
--------------------------
Transform registry using a decorator pattern.

Every transform class decorated with @register_transform is added to
TRANSFORM_REGISTRY keyed by its name. The Pipeline resolves transform
names from pipeline_config.yaml through this registry.

Usage:
    from etl.transforms.registry import register_transform

    @register_transform("NullFill")
    class NullFill(BaseTransform):
        name = "NullFill"
        ...

Then in pipeline_config.yaml:
    stages:
      - name: clean
        transforms: [NullFill, TypeCast, Dedup]
"""

from __future__ import annotations

import logging
from typing import Dict, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from etl.framework import BaseTransform

logger = logging.getLogger(__name__)

# Global registry: transform_name -> class
TRANSFORM_REGISTRY: Dict[str, Type["BaseTransform"]] = {}


def register_transform(name: str):
    """
    Class decorator that registers a BaseTransform subclass.

    Args:
        name: The string key used to reference this transform in
              pipeline_config.yaml. Must be unique across the codebase.

    Returns:
        The original class (unmodified), now present in TRANSFORM_REGISTRY.

    Raises:
        ValueError: If a transform with this name is already registered.

    Example:
        @register_transform("FxConvert")
        class FxConvert(BaseTransform):
            name = "FxConvert"
            stage = "enrich"
    """
    def decorator(cls):
        if name in TRANSFORM_REGISTRY:
            raise ValueError(
                f"Transform '{name}' is already registered "
                f"(registered by {TRANSFORM_REGISTRY[name].__module__}). "
                f"Each transform name must be unique."
            )
        cls.name = name
        TRANSFORM_REGISTRY[name] = cls
        logger.debug("Registered transform: '%s' -> %s", name, cls.__qualname__)
        return cls
    return decorator


def list_transforms() -> Dict[str, str]:
    """
    Return a summary of all registered transforms.

    Returns:
        Dict mapping transform name -> module path.
    """
    return {
        name: f"{cls.__module__}.{cls.__qualname__}"
        for name, cls in TRANSFORM_REGISTRY.items()
    }


def get_transform(name: str) -> Type["BaseTransform"]:
    """
    Retrieve a registered transform class by name.

    Args:
        name: The registered transform name.

    Returns:
        The transform class.

    Raises:
        KeyError: If no transform with this name is registered.
    """
    if name not in TRANSFORM_REGISTRY:
        available = ", ".join(sorted(TRANSFORM_REGISTRY.keys()))
        raise KeyError(
            f"Transform '{name}' not found in registry. "
            f"Available transforms: [{available}]"
        )
    return TRANSFORM_REGISTRY[name]


# ---------------------------------------------------------------------------
# Auto-import all transform modules so decorators fire at import time.
# Add new transform modules here as the codebase grows.
# ---------------------------------------------------------------------------

def _load_all_transforms():
    """
    Import all transform modules to trigger @register_transform decorators.
    Called once at framework startup.
    """
    import importlib

    modules = [
        "etl.transforms.clean",
        "etl.transforms.enrich",
        "etl.transforms.aggregate",
        "etl.transforms.load",
    ]

    for mod_path in modules:
        try:
            importlib.import_module(mod_path)
            logger.debug("Loaded transform module: %s", mod_path)
        except ImportError as exc:
            logger.warning("Could not load transform module '%s': %s", mod_path, exc)