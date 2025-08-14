"""Configuration loader with Hydra integration and datetime resolvers."""

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union

from dateparser import parse
from hydra import compose, initialize, initialize_config_dir
from hydra.core.global_hydra import GlobalHydra
from loguru import logger
from omegaconf import DictConfig, OmegaConf


def register_datetime_resolvers() -> None:
    """Register datetime resolvers: ${now}, ${date_add}, ${date_sub}, ${strftime}, ${parse_date}."""
    # Register resolver for current datetime
    OmegaConf.register_new_resolver("now", lambda: datetime.now(), replace=True)

    # Register resolver for date arithmetic - addition
    OmegaConf.register_new_resolver(
        "date_add",
        lambda base_date, days=0, hours=0, minutes=0, seconds=0: base_date
        + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds),
        replace=True,
    )

    # Register resolver for date arithmetic - subtraction
    OmegaConf.register_new_resolver(
        "date_sub",
        lambda base_date, days=0, hours=0, minutes=0, seconds=0: base_date
        - timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds),
        replace=True,
    )

    # Register resolver for date formatting
    OmegaConf.register_new_resolver(
        "strftime", lambda date_obj, fmt: date_obj.strftime(fmt), replace=True
    )

    # Register resolver to parse date strings
    OmegaConf.register_new_resolver(
        "parse_date",
        lambda date_str: datetime.fromisoformat(date_str)
        if isinstance(date_str, str)
        else date_str,
        replace=True,
    )


def _parse_config_path(
    config_file: Union[str, Path], validate_exists: bool = False
) -> tuple[str, str]:
    """Parse config file path into (config_dir, config_name)."""
    config_path = Path(config_file)

    # Validate file existence if requested
    if validate_exists and not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    config_dir = str(config_path.parent.absolute())
    config_name = config_path.stem

    return config_dir, config_name


def _setup_hydra_environment(run_time: Optional[datetime] = None) -> None:
    """Setup Hydra environment: cleanup, register resolvers, set run_time."""
    if GlobalHydra.instance().is_initialized():
        GlobalHydra.instance().clear()

    register_datetime_resolvers()
    OmegaConf.register_new_resolver("run_time", lambda: run_time, replace=True)


def load_hydra_config(
    config_dir: str,
    config_name: str = "config",
    overrides: Optional[List[str]] = None,
    run_time: Optional[datetime] = None,
) -> DictConfig:
    """Load Hydra configuration with datetime resolvers."""
    overrides = overrides or []
    run_time = run_time or datetime.now()
    _setup_hydra_environment(run_time)

    config_dir = str(config_dir)

    init_func = initialize_config_dir if os.path.isabs(config_dir) else initialize

    # Load and compose configuration
    with init_func(version_base=None, config_dir=config_dir):
        return compose(config_name=config_name, overrides=overrides)


def load_config_from_file(
    config_file: Union[str, Path],
    run_time: Optional[datetime] = None,
    overrides: Optional[List[str]] = None,
) -> Dict:
    """Load config directly from file without command-line parsing."""
    # Parse configuration file path and validate existence
    config_dir, config_name = _parse_config_path(config_file, validate_exists=True)

    # Load configuration using Hydra
    hydra_config = load_hydra_config(
        config_dir=config_dir,
        config_name=config_name,
        overrides=overrides or [],
        run_time=run_time,
    )

    hydra_config.run_time = run_time

    logger.info(f"Configuration loaded from {config_file}: {hydra_config}")
    return hydra_config


def load_config() -> Dict:
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
        "--run-time",
        type=parse,  # Uses dateparser for flexible date/time parsing
        required=False,
        help="Date and time for the ETL run (e.g., '2024-06-15 14:30:00', 'yesterday', '2 hours ago')",
    )

    parser.add_argument(
        "--overrides",
        type=str,
        nargs="*",  # Accept multiple override values
        required=False,
        help="Override configuration values in Hydra format (e.g., key=value nested.key=value)",
    )

    # Parse command-line arguments
    args = parser.parse_args()
    logger.info(f"Parsed arguments: {args}")

    return load_config_from_file(args.config_file, args.run_time, args.overrides)


if __name__ == "__main__":
    """
    Command-line interface for testing configuration loading.
    
    Usage:
        python config_loader.py --config-file path/to/config.yaml --run-time "2024-06-15 14:30:00"
    """
    config = load_config()
    logger.info(f"Loaded configuration: {config}")
