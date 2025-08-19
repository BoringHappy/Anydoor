import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pytest
from hydra.core.global_hydra import GlobalHydra
from omegaconf import OmegaConf
from pydantic import BaseModel, Field, ValidationError

from anydoor.utils.config import load_config, load_hydra_config


# Pydantic models for testing
class DatabaseConfig(BaseModel):
    """Pydantic model for database configuration."""

    host: str
    port: int = Field(gt=0, le=65535)
    name: str
    username: Optional[str] = None
    password: Optional[str] = None
    pool_size: Optional[int] = Field(default=5, gt=0)
    backup_enabled: Optional[bool] = False


class AppConfig(BaseModel):
    """Pydantic model for app configuration."""

    name: str
    version: str
    debug: bool = False
    environment: Optional[str] = "development"
    start_time: Optional[datetime] = None
    future_date: Optional[datetime] = None
    past_date: Optional[datetime] = None
    deadline: Optional[datetime] = None
    formatted_time: Optional[str] = None


class LoggingConfig(BaseModel):
    """Pydantic model for logging configuration."""

    level: str = Field(pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    format: str
    file: Optional[str] = None
    handlers: Optional[List[str]] = []


class APIConfig(BaseModel):
    """Pydantic model for API configuration."""

    host: str = "0.0.0.0"
    port: int = Field(gt=0, le=65535)
    workers: int = Field(gt=0)

    class RateLimit(BaseModel):
        enabled: bool = False
        requests_per_minute: Optional[int] = 60

    class CORS(BaseModel):
        enabled: bool = False
        origins: Optional[List[str]] = ["*"]
        headers: Optional[List[str]] = []

    rate_limit: Optional[RateLimit] = RateLimit()
    cors: Optional[CORS] = CORS()


class CompleteConfig(BaseModel):
    """Complete Pydantic model for full configuration."""

    app: AppConfig
    database: DatabaseConfig
    logging: LoggingConfig
    api: Optional[APIConfig] = None


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


class TestPydanticModelValidation:
    """Test suite for Pydantic model validation features.

    This test class validates the pydantic_model parameter functionality
    in both load_hydra_config and load_config functions. It covers:

    - Basic Pydantic model validation with complete config structures
    - Validation with config overrides
    - DateTime resolver integration with Pydantic models
    - Error handling for invalid configurations
    - Partial model validation for specific config sections
    - Interaction with to_dict parameter
    - Field-level validations (ports, pool sizes, etc.)
    - Command-line argument handling with Pydantic models
    """

    @pytest.fixture
    def temp_config_dir(self):
        """Create a temporary directory with test config files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a basic config.yaml that matches our Pydantic models
            config_content = """
app:
  name: test_app
  version: 1.0.0
  debug: false
  environment: development

database:
  host: localhost
  port: 5432
  name: test_db
  username: postgres
  password: secret
  pool_size: 10
  backup_enabled: true

logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: /var/log/app.log
  handlers: 
    - console
    - file

api:
  host: 0.0.0.0
  port: 8000
  workers: 4
  rate_limit:
    enabled: true
    requests_per_minute: 100
  cors:
    enabled: true
    origins: 
      - http://localhost:3000
      - http://localhost:8080
    headers:
      - Content-Type
      - Authorization
"""
            config_file = temp_path / "config.yaml"
            config_file.write_text(config_content.strip())

            # Create a config with datetime resolvers
            datetime_config_content = """
app:
  name: datetime_test_app
  version: 2.0.0
  debug: false
  environment: test
  start_time: ${dt.now:}
  future_date: ${dt.add:${dt.now:},7,0,0,0}
  past_date: ${dt.sub:${dt.now:},3,0,0,0}
  deadline: ${dt.parse:2024-12-31T23:59:59}
  formatted_time: ${dt.format:${dt.now:},%Y-%m-%d %H:%M:%S}

database:
  host: localhost
  port: 5432
  name: datetime_test_db

logging:
  level: DEBUG
  format: "%(message)s"
"""
            datetime_config_file = temp_path / "datetime_config.yaml"
            datetime_config_file.write_text(datetime_config_content.strip())

            # Create an invalid config for error testing
            invalid_config_content = """
app:
  name: invalid_app
  version: 1.0.0
  debug: not_a_boolean  # Invalid boolean
  environment: development

database:
  host: localhost
  port: 99999  # Invalid port (too high)
  name: test_db

logging:
  level: INVALID_LEVEL  # Invalid log level
  format: "%(message)s"
"""
            invalid_config_file = temp_path / "invalid_config.yaml"
            invalid_config_file.write_text(invalid_config_content.strip())

            yield str(temp_path)

    def teardown_method(self):
        """Clean up GlobalHydra instance after each test."""
        if GlobalHydra.instance().is_initialized():
            GlobalHydra.instance().clear()

    def test_basic_pydantic_validation(self, temp_config_dir):
        """Test basic Pydantic model validation with load_hydra_config."""
        config = load_hydra_config(
            config_dir=temp_config_dir,
            config_name="config",
            pydantic_model=CompleteConfig,
        )

        # Verify the returned object is a Pydantic model instance
        assert isinstance(config, CompleteConfig)

        # Test app section
        assert config.app.name == "test_app"
        assert config.app.version == "1.0.0"
        assert config.app.debug is False
        assert config.app.environment == "development"

        # Test database section with validation
        assert config.database.host == "localhost"
        assert config.database.port == 5432
        assert config.database.name == "test_db"
        assert config.database.username == "postgres"
        assert config.database.password == "secret"
        assert config.database.pool_size == 10
        assert config.database.backup_enabled is True

        # Test logging section
        assert config.logging.level == "INFO"
        assert "%(asctime)s" in config.logging.format
        assert config.logging.file == "/var/log/app.log"
        assert len(config.logging.handlers) == 2

        # Test API section
        assert config.api is not None
        assert config.api.host == "0.0.0.0"
        assert config.api.port == 8000
        assert config.api.workers == 4
        assert config.api.rate_limit.enabled is True
        assert config.api.rate_limit.requests_per_minute == 100
        assert config.api.cors.enabled is True
        assert len(config.api.cors.origins) == 2

    def test_pydantic_validation_with_overrides(self, temp_config_dir):
        """Test Pydantic model validation with config overrides."""
        overrides = [
            "app.debug=true",
            "database.port=3306",
            "api.workers=8",
            "logging.level=DEBUG",
        ]

        config = load_hydra_config(
            config_dir=temp_config_dir,
            config_name="config",
            overrides=overrides,
            pydantic_model=CompleteConfig,
        )

        # Verify overrides are applied and validated
        assert isinstance(config, CompleteConfig)
        assert config.app.debug is True
        assert config.database.port == 3306
        assert config.api.workers == 8
        assert config.logging.level == "DEBUG"

        # Verify non-overridden values remain
        assert config.app.name == "test_app"
        assert config.database.host == "localhost"

    def test_pydantic_datetime_validation(self, temp_config_dir):
        """Test Pydantic model validation with datetime resolvers."""
        config = load_hydra_config(
            config_dir=temp_config_dir,
            config_name="datetime_config",
            pydantic_model=CompleteConfig,
        )

        assert isinstance(config, CompleteConfig)

        # Test datetime fields are properly resolved and validated
        assert isinstance(config.app.start_time, datetime)
        assert isinstance(config.app.future_date, datetime)
        assert isinstance(config.app.past_date, datetime)
        assert isinstance(config.app.deadline, datetime)
        assert isinstance(config.app.formatted_time, str)

        # Test datetime arithmetic
        assert config.app.future_date > config.app.start_time
        assert config.app.past_date < config.app.start_time

        # Test specific parsed date
        assert config.app.deadline.year == 2024
        assert config.app.deadline.month == 12
        assert config.app.deadline.day == 31

    def test_pydantic_validation_errors(self, temp_config_dir):
        """Test Pydantic validation error handling for invalid configs."""
        with pytest.raises(ValidationError) as exc_info:
            load_hydra_config(
                config_dir=temp_config_dir,
                config_name="invalid_config",
                pydantic_model=CompleteConfig,
            )

        # Check that validation errors are properly raised
        validation_error = exc_info.value
        assert len(validation_error.errors()) > 0

        # Check specific validation errors
        errors = {
            error["loc"][0] if error["loc"] else "root": error["type"]
            for error in validation_error.errors()
        }

        # We expect errors for invalid port and log level
        assert any("database" in str(loc) for loc in errors.keys())
        assert any("logging" in str(loc) for loc in errors.keys())

    def test_partial_pydantic_models(self, temp_config_dir):
        """Test using partial Pydantic models for specific config sections."""
        # Create a config file with just database section for testing
        database_only_config = """
host: localhost
port: 5432
name: test_db
username: postgres
password: secret
pool_size: 10
backup_enabled: true
"""
        db_config_file = Path(temp_config_dir) / "database_only.yaml"
        db_config_file.write_text(database_only_config.strip())

        # Test with just DatabaseConfig
        config = load_hydra_config(
            config_dir=temp_config_dir,
            config_name="database_only",
            pydantic_model=DatabaseConfig,
        )

        assert isinstance(config, DatabaseConfig)
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.pool_size == 10

        # Clean up for next test
        if GlobalHydra.instance().is_initialized():
            GlobalHydra.instance().clear()

        # Create a config file with just app section
        app_only_config = """
name: test_app
version: 1.0.0
debug: false
environment: development
"""
        app_config_file = Path(temp_config_dir) / "app_only.yaml"
        app_config_file.write_text(app_only_config.strip())

        # Test with just AppConfig
        app_config = load_hydra_config(
            config_dir=temp_config_dir, config_name="app_only", pydantic_model=AppConfig
        )

        assert isinstance(app_config, AppConfig)
        assert app_config.name == "test_app"
        assert app_config.version == "1.0.0"

    def test_pydantic_with_to_dict_parameter(self, temp_config_dir):
        """Test behavior when both pydantic_model and to_dict are specified."""
        # Based on current implementation, to_dict takes precedence over pydantic_model
        # This test documents the current behavior
        config = load_hydra_config(
            config_dir=temp_config_dir,
            config_name="config",
            to_dict=True,
            pydantic_model=CompleteConfig,  # This is ignored when to_dict=True
        )

        # Should return dict, not Pydantic model (current implementation behavior)
        assert isinstance(config, dict)
        assert not isinstance(config, CompleteConfig)
        assert config["app"]["name"] == "test_app"
        assert config["database"]["host"] == "localhost"

        # Test that pydantic_model works when to_dict=False
        if GlobalHydra.instance().is_initialized():
            GlobalHydra.instance().clear()

        pydantic_config = load_hydra_config(
            config_dir=temp_config_dir,
            config_name="config",
            to_dict=False,
            pydantic_model=CompleteConfig,
        )

        assert isinstance(pydantic_config, CompleteConfig)
        assert pydantic_config.app.name == "test_app"

    def test_pydantic_field_validation(self, temp_config_dir):
        """Test specific Pydantic field validations."""
        # Test port validation with invalid high port
        with pytest.raises(ValidationError):
            overrides = ["database.port=99999"]  # Too high
            load_hydra_config(
                config_dir=temp_config_dir,
                config_name="config",
                overrides=overrides,
                pydantic_model=CompleteConfig,
            )

        # Test port validation with invalid low port
        with pytest.raises(ValidationError):
            overrides = ["database.port=0"]  # Too low
            load_hydra_config(
                config_dir=temp_config_dir,
                config_name="config",
                overrides=overrides,
                pydantic_model=CompleteConfig,
            )

        # Test pool_size validation
        with pytest.raises(ValidationError):
            overrides = ["database.pool_size=0"]  # Should be > 0
            load_hydra_config(
                config_dir=temp_config_dir,
                config_name="config",
                overrides=overrides,
                pydantic_model=CompleteConfig,
            )

    def test_load_config_with_pydantic_model(self, temp_config_dir, monkeypatch):
        """Test load_config function with pydantic_model parameter."""
        # Mock sys.argv to simulate command line arguments
        test_args = [
            "test_script.py",
            "--config-file",
            str(Path(temp_config_dir) / "config.yaml"),
            "--override",
            "app.debug=true",
            "database.port=3306",
        ]

        monkeypatch.setattr("sys.argv", test_args)

        config = load_config(pydantic_model=CompleteConfig)

        assert isinstance(config, CompleteConfig)
        assert config.app.name == "test_app"
        assert config.app.debug is True  # Override applied
        assert config.database.port == 3306  # Override applied
        assert config.database.host == "localhost"  # Original value

    def test_pydantic_model_with_config_file_parameter(self, temp_config_dir):
        """Test pydantic_model with config_file parameter."""
        config_file_path = Path(temp_config_dir) / "config.yaml"

        config = load_hydra_config(
            config_file=config_file_path, pydantic_model=CompleteConfig
        )

        assert isinstance(config, CompleteConfig)
        assert config.app.name == "test_app"
        assert config.database.host == "localhost"

    def test_pydantic_model_none_handling(self, temp_config_dir):
        """Test behavior when pydantic_model is None (should return DictConfig)."""
        config = load_hydra_config(
            config_dir=temp_config_dir, config_name="config", pydantic_model=None
        )

        # Should return OmegaConf DictConfig, not Pydantic model
        from omegaconf import DictConfig

        assert isinstance(config, DictConfig)
        assert not isinstance(config, BaseModel)


if __name__ == "__main__":
    pytest.main([__file__])
