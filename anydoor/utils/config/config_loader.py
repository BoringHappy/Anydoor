"""Configuration loader with Hydra integration and datetime resolvers."""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Union

from hydra import compose, initialize_config_dir
from hydra.core.global_hydra import GlobalHydra
from loguru import logger
from omegaconf import DictConfig, OmegaConf


def register_datetime_resolvers() -> None:
    """Register datetime resolvers: ${dt.now}, ${dt.add}, ${dt.sub}, ${dt.format}, ${dt.parse}."""
    # Register resolver for current datetime
    OmegaConf.register_new_resolver("dt.now", lambda: datetime.now(), replace=True)

    # Register resolver for date arithmetic - addition
    OmegaConf.register_new_resolver(
        "dt.add",
        lambda base_date, days=0, hours=0, minutes=0, seconds=0: base_date
        + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds),
        replace=True,
    )

    # Register resolver for date arithmetic - subtraction
    OmegaConf.register_new_resolver(
        "dt.sub",
        lambda base_date, days=0, hours=0, minutes=0, seconds=0: base_date
        - timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds),
        replace=True,
    )

    # Register resolver for date formatting
    OmegaConf.register_new_resolver(
        "dt.format", lambda date_obj, fmt: date_obj.strftime(fmt), replace=True
    )

    # Register resolver to parse date strings
    OmegaConf.register_new_resolver(
        "dt.parse",
        lambda date_str: datetime.fromisoformat(date_str)
        if isinstance(date_str, str)
        else date_str,
        replace=True,
    )


def _parse_config_path(config_file: Union[str, Path]) -> tuple[str, str]:
    """Parse config file path into (config_dir, config_name)."""
    config_path = Path(str(config_file))

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    config_dir = str(config_path.parent.absolute())
    config_name = config_path.stem

    return config_dir, config_name


def _setup_hydra_environment() -> None:
    """Setup Hydra environment: cleanup, register resolvers, set run_time."""
    if GlobalHydra.instance().is_initialized():
        GlobalHydra.instance().clear()

    register_datetime_resolvers()


def load_hydra_config(
    config_dir: Optional[Union[str, Path]] = None,
    config_name: Optional[str] = None,
    config_file: Optional[Union[str, Path]] = None,
    overrides: Optional[List[str]] = None,
) -> DictConfig:
    """Load Hydra configuration with datetime resolvers."""

    if config_file:
        config_dir, config_name = _parse_config_path(config_file)

    overrides = overrides or []
    _setup_hydra_environment()

    config_dir = str(Path(config_dir).absolute())

    with initialize_config_dir(version_base=None, config_dir=config_dir):
        return compose(config_name=config_name, overrides=overrides)


def load_config() -> DictConfig:
    """Load config from command-line args. Supports --config-file, --run-time, --override."""
    # Set up command-line argument parser
    parser = argparse.ArgumentParser(
        description="Configuration loader with Hydra and datetime support"
    )

    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to the YAML configuration file",
    )

    parser.add_argument(
        "--override",
        type=str,
        nargs="*",  # Accept multiple override values
        required=False,
        help="Override configuration values in Hydra format (e.g., key=value nested.key=value)",
    )

    # Parse command-line arguments
    args = parser.parse_args()
    logger.info(f"Parsed arguments: {args}")

    return load_hydra_config(
        config_file=args.config_file,
        overrides=args.override,
    )


if __name__ == "__main__":
    config = load_config()
    logger.info(f"Loaded configuration: {config}")
