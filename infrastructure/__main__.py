"""Analysis-runner infrastructure."""

from components.metamist_consumer import create_metamist_consumer_resources
from components.server import create_server_resources
from components.web import create_web_resources

server = create_server_resources()
_web = create_web_resources()
_consumer = create_metamist_consumer_resources(
    submissions_topic=server['submissions_topic'],
)
