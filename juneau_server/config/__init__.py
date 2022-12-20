import os
from omegaconf import OmegaConf

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
config = OmegaConf.load(CONFIG_PATH)
