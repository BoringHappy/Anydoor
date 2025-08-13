from hydra import initialize_config_dir, compose,initialize
from hydra.core.global_hydra import GlobalHydra
import os

def load_hydra(config_dir: str, config_name: str = "config", overrides=None):
    overrides = overrides or []
    if GlobalHydra.instance().is_initialized():
        GlobalHydra.instance().clear()
    
    # Convert Path objects to strings
    config_dir = str(config_dir)
    
    # Use initialize_config_dir for absolute paths, initialize for relative paths
    init_func = initialize_config_dir if os.path.isabs(config_dir) else initialize
    with init_func(version_base=None, config_dir=config_dir):
        return compose(config_name=config_name, overrides=overrides)
