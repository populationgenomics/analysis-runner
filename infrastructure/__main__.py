"""Analysis-runner infrastructure."""

from components.server import create_server_resources
from components.web import create_web_resources

_server = create_server_resources()
_web = create_web_resources()
