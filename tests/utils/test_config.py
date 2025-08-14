import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from hydra.core.global_hydra import GlobalHydra
from omegaconf import OmegaConf

from anydoor.utils.config import load_hydra_config


class TestLoadHydra:
    """Test suite for the load_hydra function."""

    @pytest.fixture
    def temp_config_dir(self):
        """Create a temporary directory with test config files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a basic config.yaml
            config_content = """
app:
  name: test_app
  version: 1.0.0
  debug: false

database:
  host: localhost
  port: 5432
  name: test_db

logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
"""
            config_file = temp_path / "config.yaml"
            config_file.write_text(config_content.strip())

            # Create an alternative config
            alt_config_content = """
app:
  name: alt_app
  version: 2.0.0
  debug: true

database:
  host: remote_host
  port: 3306
  name: alt_db
"""
            alt_config_file = temp_path / "alt_config.yaml"
            alt_config_file.write_text(alt_config_content.strip())

            yield str(temp_path)

    def teardown_method(self):
        """Clean up GlobalHydra instance after each test."""
        if GlobalHydra.instance().is_initialized():
            GlobalHydra.instance().clear()

    def test_load_basic_config(self, temp_config_dir):
        """Test loading a basic configuration."""
        config = load_hydra_config(config_dir=temp_config_dir, config_name="config")

        assert config is not None
        assert config.app.name == "test_app"
        assert config.app.version == "1.0.0"
        assert config.app.debug is False
        assert config.database.host == "localhost"
        assert config.database.port == 5432
        assert config.database.name == "test_db"
        assert config.logging.level == "INFO"

    def test_load_alternative_config(self, temp_config_dir):
        """Test loading an alternative configuration file."""
        config = load_hydra_config(config_dir=temp_config_dir, config_name="alt_config")

        assert config is not None
        assert config.app.name == "alt_app"
        assert config.app.version == "2.0.0"
        assert config.app.debug is True
        assert config.database.host == "remote_host"
        assert config.database.port == 3306
        assert config.database.name == "alt_db"

    def test_load_config_with_overrides(self, temp_config_dir):
        """Test loading configuration with overrides."""
        overrides = ["app.name=overridden_app", "app.debug=true", "database.port=9999"]

        config = load_hydra_config(
            config_dir=temp_config_dir, config_name="config", overrides=overrides
        )

        assert config is not None
        assert config.app.name == "overridden_app"
        assert config.app.debug is True
        assert config.database.port == 9999
        # Non-overridden values should remain the same
        assert config.app.version == "1.0.0"
        assert config.database.host == "localhost"
        assert config.database.name == "test_db"

    def test_load_config_with_empty_overrides(self, temp_config_dir):
        """Test loading configuration with empty overrides list."""
        config = load_hydra_config(
            config_dir=temp_config_dir, config_name="config", overrides=[]
        )

        assert config is not None
        assert config.app.name == "test_app"
        assert config.app.version == "1.0.0"
        assert config.app.debug is False

    def test_load_config_with_none_overrides(self, temp_config_dir):
        """Test loading configuration with None overrides (default behavior)."""
        config = load_hydra_config(
            config_dir=temp_config_dir, config_name="config", overrides=None
        )

        assert config is not None
        assert config.app.name == "test_app"
        assert config.app.version == "1.0.0"
        assert config.app.debug is False

    def test_global_hydra_cleanup(self, temp_config_dir):
        """Test that GlobalHydra instance is properly cleared when already initialized."""
        # Initialize GlobalHydra manually to test cleanup
        from hydra import initialize_config_dir

        # Initialize and keep it initialized (don't use context manager)
        init_ctx = initialize_config_dir(version_base=None, config_dir=temp_config_dir)
        init_ctx.__enter__()

        try:
            # Verify it's initialized
            assert GlobalHydra.instance().is_initialized()

            # load_hydra should clear the existing instance and work correctly
            config1 = load_hydra_config(
                config_dir=temp_config_dir, config_name="config"
            )
            assert config1.app.name == "test_app"

            # Test multiple consecutive loads work
            config2 = load_hydra_config(
                config_dir=temp_config_dir, config_name="alt_config"
            )
            assert config2.app.name == "alt_app"
        finally:
            # Clean up manually
            if GlobalHydra.instance().is_initialized():
                GlobalHydra.instance().clear()

    def test_multiple_consecutive_loads(self, temp_config_dir):
        """Test multiple consecutive config loads."""
        # Load first config
        config1 = load_hydra_config(config_dir=temp_config_dir, config_name="config")
        assert config1.app.name == "test_app"

        # Load second config with overrides
        overrides = ["app.name=consecutive_test"]
        config2 = load_hydra_config(
            config_dir=temp_config_dir, config_name="config", overrides=overrides
        )
        assert config2.app.name == "consecutive_test"

        # Load third config (different file)
        config3 = load_hydra_config(
            config_dir=temp_config_dir, config_name="alt_config"
        )
        assert config3.app.name == "alt_app"

    def test_invalid_config_directory(self):
        """Test behavior with invalid config directory."""
        with pytest.raises(Exception):
            load_hydra_config(config_dir="/nonexistent/directory", config_name="config")

    def test_invalid_config_name(self, temp_config_dir):
        """Test behavior with invalid config name."""
        with pytest.raises(Exception):
            load_hydra_config(
                config_dir=temp_config_dir, config_name="nonexistent_config"
            )

    def test_config_directory_path_types(self, temp_config_dir):
        """Test that both string and Path objects work for config_dir."""
        # Test with string path
        config1 = load_hydra_config(config_dir=temp_config_dir, config_name="config")
        assert config1.app.name == "test_app"

        # Clean up for next test
        if GlobalHydra.instance().is_initialized():
            GlobalHydra.instance().clear()

        # Test with Path object
        config2 = load_hydra_config(
            config_dir=Path(temp_config_dir), config_name="config"
        )
        assert config2.app.name == "test_app"


class TestDatetimeResolvers:
    """Test suite for datetime resolvers functionality."""

    @pytest.fixture
    def temp_config_dir(self):
        """Create a temporary directory with test config files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            yield str(temp_path)

    def teardown_method(self):
        """Clean up after each test."""
        if GlobalHydra.instance().is_initialized():
            GlobalHydra.instance().clear()

    def test_now_resolver(self, temp_config_dir):
        """Test the ${dt.now:} resolver."""
        config_content = """
current_time: ${dt.now:}
"""
        config_file = Path(temp_config_dir) / "test_now.yaml"
        config_file.write_text(config_content.strip())

        config = load_hydra_config(config_file=config_file)

        # Access the resolved value directly from config
        current_time = config.current_time

        # Check that it's a datetime object
        assert isinstance(current_time, datetime)

        # Check that it's approximately now (within 1 second)
        now = datetime.now()
        time_diff = abs((current_time - now).total_seconds())
        assert time_diff < 1.0

    def test_date_add_resolver(self, temp_config_dir):
        """Test the ${dt.add} resolver."""
        config_content = """
base_date: ${dt.now:}
future_date: ${dt.add:${dt.now:},7,2,30,0}
"""
        config_file = Path(temp_config_dir) / "test_add.yaml"
        config_file.write_text(config_content.strip())

        config = load_hydra_config(config_file=config_file)

        base_date = config.base_date
        future_date = config.future_date

        expected_delta = timedelta(days=7, hours=2, minutes=30)
        actual_delta = future_date - base_date

        # Allow small difference due to execution time
        assert abs((actual_delta - expected_delta).total_seconds()) < 1.0

    def test_date_sub_resolver(self, temp_config_dir):
        """Test the ${dt.sub} resolver."""
        config_content = """
base_date: ${dt.now:}
past_date: ${dt.sub:${dt.now:},5,1,0,0}
"""
        config_file = Path(temp_config_dir) / "test_sub.yaml"
        config_file.write_text(config_content.strip())

        config = load_hydra_config(config_file=config_file)

        base_date = config.base_date
        past_date = config.past_date

        expected_delta = timedelta(days=5, hours=1)
        actual_delta = base_date - past_date

        # Allow small difference due to execution time
        assert abs((actual_delta - expected_delta).total_seconds()) < 1.0

    def test_strftime_resolver(self, temp_config_dir):
        """Test the ${dt.format} resolver."""
        config_content = """
formatted_date: ${dt.format:${dt.now:},%Y-%m-%d}
formatted_datetime: ${dt.format:${dt.now:},%Y-%m-%d %H:%M:%S}
"""
        config_file = Path(temp_config_dir) / "test_strftime.yaml"
        config_file.write_text(config_content.strip())

        config = load_hydra_config(config_file=config_file)

        # Check format matches expected pattern
        formatted_date = config.formatted_date
        formatted_datetime = config.formatted_datetime

        # Verify format
        assert len(formatted_date) == 10  # YYYY-MM-DD
        assert len(formatted_datetime) == 19  # YYYY-MM-DD HH:MM:SS
        assert "-" in formatted_date
        assert ":" in formatted_datetime

    def test_parse_date_resolver(self, temp_config_dir):
        """Test the ${dt.parse} resolver."""
        config_content = """
parsed_date: ${dt.parse:2024-12-31T23:59:59}
parsed_simple: ${dt.parse:2024-01-01}
"""
        config_file = Path(temp_config_dir) / "test_parse.yaml"
        config_file.write_text(config_content.strip())

        config = load_hydra_config(config_file=config_file)

        parsed_date = config.parsed_date
        parsed_simple = config.parsed_simple

        assert isinstance(parsed_date, datetime)
        assert isinstance(parsed_simple, datetime)
        assert parsed_date.year == 2024
        assert parsed_date.month == 12
        assert parsed_date.day == 31
        assert parsed_date.hour == 23
        assert parsed_date.minute == 59
        assert parsed_date.second == 59

    def test_complex_datetime_operations(self, temp_config_dir):
        """Test complex combinations of datetime resolvers."""
        config_content = """
# Parse a base date and add time to it
base: ${dt.parse:2024-01-01T00:00:00}
future: ${dt.add:${dt.parse:2024-01-01T00:00:00},30,0,0,0}
formatted_future: ${dt.format:${dt.add:${dt.parse:2024-01-01T00:00:00},30,0,0,0},%Y-%m-%d}
"""
        config_file = Path(temp_config_dir) / "test_complex.yaml"
        config_file.write_text(config_content.strip())

        config = load_hydra_config(config_file=config_file)

        base = config.base
        future = config.future
        formatted_future = config.formatted_future

        assert isinstance(base, datetime)
        assert isinstance(future, datetime)
        assert isinstance(formatted_future, str)

        # Check that future is 30 days after base
        delta = future - base
        assert delta.days == 30

        # Check formatted date
        assert formatted_future == "2024-01-31"


class TestExampleConfig:
    """Test suite for the example configuration file."""

    @pytest.fixture
    def example_config_path(self):
        """Get path to the example config file."""
        return Path(__file__).parent / "assets" / "example_config.yaml"

    def teardown_method(self):
        """Clean up GlobalHydra instance after each test."""
        if GlobalHydra.instance().is_initialized():
            GlobalHydra.instance().clear()

    def test_example_config_loads(self, example_config_path):
        """Test that the example config file loads successfully."""
        assert example_config_path.exists(), (
            f"Example config not found at {example_config_path}"
        )

        config = load_hydra_config(config_file=example_config_path)

        assert config is not None
        assert hasattr(config, "app")
        assert hasattr(config, "database")
        assert hasattr(config, "logging")

    def test_example_config_app_section(self, example_config_path):
        """Test the app section of example config."""
        config = load_hydra_config(config_file=example_config_path)

        assert config.app.name == "anydoor_example"
        assert config.app.version == "1.0.0"
        assert config.app.debug is False
        assert config.app.environment == "development"

        # Test datetime resolvers work
        assert hasattr(config.app, "start_time")
        assert hasattr(config.app, "formatted_time")
        assert hasattr(config.app, "future_date")
        assert hasattr(config.app, "past_date")

    def test_example_config_database_section(self, example_config_path):
        """Test the database section of example config."""
        config = load_hydra_config(config_file=example_config_path)

        assert config.database.host == "localhost"
        assert config.database.port == 5432
        assert config.database.name == "anydoor_db"
        assert config.database.username == "postgres"
        assert config.database.pool_size == 10
        assert config.database.backup_enabled is True

    def test_example_config_logging_section(self, example_config_path):
        """Test the logging section of example config."""
        config = load_hydra_config(config_file=example_config_path)

        assert config.logging.level == "INFO"
        assert "%(asctime)s" in config.logging.format
        assert hasattr(config.logging, "file")
        assert hasattr(config.logging, "handlers")
        assert len(config.logging.handlers) == 2

    def test_example_config_api_section(self, example_config_path):
        """Test the API section of example config."""
        config = load_hydra_config(config_file=example_config_path)

        assert config.api.host == "0.0.0.0"
        assert config.api.port == 8000
        assert config.api.workers == 4
        assert config.api.rate_limit.enabled is True
        assert config.api.cors.enabled is True

    def test_example_config_with_overrides(self, example_config_path):
        """Test example config with command-line overrides."""
        overrides = ["app.debug=true", "database.port=5433", "api.workers=8"]

        config = load_hydra_config(config_file=example_config_path, overrides=overrides)

        # Check overrides applied
        assert config.app.debug is True
        assert config.database.port == 5433
        assert config.api.workers == 8

        # Check non-overridden values remain
        assert config.app.name == "anydoor_example"
        assert config.database.host == "localhost"

    def test_example_config_datetime_features(self, example_config_path):
        """Test datetime-related features in example config."""
        config = load_hydra_config(config_file=example_config_path)

        # Test that datetime resolvers produced actual datetime objects
        resolved_config = OmegaConf.to_container(config, resolve=True)

        # Check various datetime fields exist and are properly resolved
        assert "start_time" in resolved_config["app"]
        assert "future_date" in resolved_config["app"]
        assert "past_date" in resolved_config["app"]
        assert "deadline" in resolved_config["app"]

        # Check scheduler job has next_run field
        assert "next_run" in resolved_config["scheduler"]["jobs"][1]

    def test_example_config_nested_structures(self, example_config_path):
        """Test complex nested structures in example config."""
        config = load_hydra_config(config_file=example_config_path)

        # Test services configuration
        assert hasattr(config.services, "redis")
        assert hasattr(config.services, "elasticsearch")
        assert hasattr(config.services, "email")

        # Test features section
        assert hasattr(config.features, "new_ui")
        assert hasattr(config.features, "advanced_analytics")

        # Test monitoring alerts
        assert len(config.monitoring.alerts.email_recipients) == 2
        assert config.monitoring.alerts.thresholds.cpu_usage == 80


if __name__ == "__main__":
    pytest.main([__file__])
