import tempfile
import pytest
import os
from pathlib import Path
from hydra.core.global_hydra import GlobalHydra
from anydoor.utils.config import load_hydra


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
        config = load_hydra(temp_config_dir, "config")
        
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
        config = load_hydra(temp_config_dir, "alt_config")
        
        assert config is not None
        assert config.app.name == "alt_app"
        assert config.app.version == "2.0.0"
        assert config.app.debug is True
        assert config.database.host == "remote_host"
        assert config.database.port == 3306
        assert config.database.name == "alt_db"
    
    def test_load_config_with_overrides(self, temp_config_dir):
        """Test loading configuration with overrides."""
        overrides = [
            "app.name=overridden_app",
            "app.debug=true",
            "database.port=9999"
        ]
        
        config = load_hydra(temp_config_dir, "config", overrides=overrides)
        
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
        config = load_hydra(temp_config_dir, "config", overrides=[])
        
        assert config is not None
        assert config.app.name == "test_app"
        assert config.app.version == "1.0.0"
        assert config.app.debug is False
    
    def test_load_config_with_none_overrides(self, temp_config_dir):
        """Test loading configuration with None overrides (default behavior)."""
        config = load_hydra(temp_config_dir, "config", overrides=None)
        
        assert config is not None
        assert config.app.name == "test_app"
        assert config.app.version == "1.0.0"
        assert config.app.debug is False
    
    def test_global_hydra_cleanup(self, temp_config_dir):
        """Test that GlobalHydra instance is properly cleared when already initialized."""
        # Initialize GlobalHydra manually to test cleanup
        from hydra import initialize_config_dir, compose
        
        # Initialize and keep it initialized (don't use context manager)
        init_ctx = initialize_config_dir(version_base=None, config_dir=temp_config_dir)
        init_ctx.__enter__()
        
        try:
            # Verify it's initialized
            assert GlobalHydra.instance().is_initialized()
            
            # load_hydra should clear the existing instance and work correctly
            config1 = load_hydra(temp_config_dir, "config")
            assert config1.app.name == "test_app"
            
            # Test multiple consecutive loads work
            config2 = load_hydra(temp_config_dir, "alt_config")
            assert config2.app.name == "alt_app"
        finally:
            # Clean up manually
            if GlobalHydra.instance().is_initialized():
                GlobalHydra.instance().clear()
    
    def test_multiple_consecutive_loads(self, temp_config_dir):
        """Test multiple consecutive config loads."""
        # Load first config
        config1 = load_hydra(temp_config_dir, "config")
        assert config1.app.name == "test_app"
        
        # Load second config with overrides
        overrides = ["app.name=consecutive_test"]
        config2 = load_hydra(temp_config_dir, "config", overrides=overrides)
        assert config2.app.name == "consecutive_test"
        
        # Load third config (different file)
        config3 = load_hydra(temp_config_dir, "alt_config")
        assert config3.app.name == "alt_app"
    
    def test_invalid_config_directory(self):
        """Test behavior with invalid config directory."""
        with pytest.raises(Exception):
            load_hydra("/nonexistent/directory", "config")
    
    def test_invalid_config_name(self, temp_config_dir):
        """Test behavior with invalid config name."""
        with pytest.raises(Exception):
            load_hydra(temp_config_dir, "nonexistent_config")
    
    def test_config_directory_path_types(self, temp_config_dir):
        """Test that both string and Path objects work for config_dir."""
        # Test with string path
        config1 = load_hydra(temp_config_dir, "config")
        assert config1.app.name == "test_app"
        
        # Clean up for next test
        if GlobalHydra.instance().is_initialized():
            GlobalHydra.instance().clear()
        
        # Test with Path object
        config2 = load_hydra(Path(temp_config_dir), "config")
        assert config2.app.name == "test_app"


if __name__ == "__main__":
    pytest.main([__file__])
