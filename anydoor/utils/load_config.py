import argparse
import json

import yaml
from dateparser import parse
from jinja2 import Template
from jsonmerge import merge
from loguru import logger


def load_config():
    parser = argparse.ArgumentParser(description="Postgres to Clickhouse ETL")
    parser.add_argument(
        "--config-file",
        type=str,
        required=False,
        help="Configuration file path",
    )
    parser.add_argument(
        "--run-time",
        type=parse,
        required=False,
        help="Date and time for the ETL run",
    )
    parser.add_argument(
        "--override",
        type=str,
        required=False,
        help="Override configuration values in the format key=value",
    )

    args = parser.parse_args()

    logger.info(f"args: {args}")

    if args.config_file:
        with open(args.config_file, "r") as f:
            config = yaml.safe_load(Template(f.read()).render(run_time=args.run_time))
            f.close()
    else:
        config = dict()

    config["run_time"] = args.run_time

    if args.override:
        overrides = json.loads(args.override)
        config = merge(config, overrides)

    logger.info(f"Configuration: {config}")

    return config


if __name__ == "__main__":
    config = load_config()
    logger.info(f"Loaded configuration: {config}")
