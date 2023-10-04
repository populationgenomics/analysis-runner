from hailtop.config import get_deploy_config

config = get_deploy_config()


print(config.get_config())
print(config.base_url('batch'))
