import os

from .common import config  as default_config
from .development import config as dev_config
from .production import config as prod_config

environment = os.environ['PYTHON_ENV'] or 'development'
environment_config = None
if environment == 'development':
    environment_config = dev_config
if environment == 'production':
    environment_config = prod_config


config = {**environment_config, **default_config}
