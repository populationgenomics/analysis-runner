"""
Analysis-runner infrastructure.
Each component is maintained using a separate stack.
The common (shared by server and web), contains resources common to both server and web.
"""

import pulumi

component = pulumi.Config().require('component')

if component == 'common':
    from components.common import create_common_resources

    create_common_resources()
elif component == 'server':
    from components.server import create_server_resources

    create_server_resources()
elif component == 'web':
    from components.web import create_web_resources

    create_web_resources()
else:
    raise ValueError(f'Unknown component {component!r}')
