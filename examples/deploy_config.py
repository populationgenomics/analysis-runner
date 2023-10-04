from hail import version
from hailtop.config import get_deploy_config

config = get_deploy_config()

print(version())
print(config.get_config())
print(config.external_url('batch', 'batches'))
print(config.base_url('batch'))
