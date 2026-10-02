"""
Web proxies (`main-web`, `test-web`)
"""

from dataclasses import dataclass

import pulumi
import pulumi_gcp as gcp

gcp_config = pulumi.Config('gcp')
PROJECT = gcp_config.require('project')
REGION = gcp_config.require('region')

config = pulumi.Config()
PROJECT_NUMBER = config.require('project_number')
MEMBERS_CACHE_LOCATION = config.require('members_cache_location')

IAP_ACCESSORS: list[str] = config.require_object('iap_accessors')
WEB_IMAGE_TAG = config.get('web_image_tag')

if not WEB_IMAGE_TAG:
    raise ValueError('Missing web_image_tag config')


@dataclass(frozen=True)
class WebProxySpec:
    name: str
    domain: str
    ip_name: str
    ip_address: str
    backend_service_id: str
    oauth2_client_id: str
    cpu: str
    memory: str
    container_name: str | None = None
    service_max_instances: int | None = None


# main and test web proxies
WEB_PROXIES = [WebProxySpec(**spec) for spec in config.require_object('web_proxies')]


def create_web_resources() -> dict[str, pulumi.Resource]:
    web_sa = gcp.serviceaccount.Account(
        'web-server-sa',
        account_id='web-server',
        display_name='web-server',
        description='Used for running the web server that serves from datasets\' "web" buckets',
        opts=pulumi.ResourceOptions(protect=True),
    )
    gcp.projects.IAMMember(
        'web-server-log-writer',
        project=PROJECT,
        role='roles/logging.logWriter',
        member=web_sa.email.apply(lambda e: f'serviceAccount:{e}'),
        opts=pulumi.ResourceOptions(protect=True),
    )

    # Shared by both HTTPS proxies (main-web, test-web)
    tls_policy = gcp.compute.SSLPolicy(
        'restricted-tls-policy',
        name='restricted-tls-policy',
        project=PROJECT,
        profile='RESTRICTED',
        min_tls_version='TLS_1_2',
        opts=pulumi.ResourceOptions(protect=True),
    )

    resources: dict[str, pulumi.Resource] = {'web_sa': web_sa, 'tls_policy': tls_policy}
    for spec in WEB_PROXIES:
        resources.update(_create_web_proxy(spec, web_sa, tls_policy))
    return resources


def _create_web_proxy(
    spec: WebProxySpec,
    web_sa: gcp.serviceaccount.Account,
    tls_policy: gcp.compute.SSLPolicy,
) -> dict[str, pulumi.Resource]:
    spec_name = spec.name

    service = gcp.cloudrunv2.Service(
        spec_name,
        name=spec_name,
        location=REGION,
        ingress='INGRESS_TRAFFIC_ALL',
        deletion_protection=True,
        scaling=(
            gcp.cloudrunv2.ServiceScalingArgs(
                max_instance_count=spec.service_max_instances
            )
            if spec.service_max_instances
            else None
        ),
        template=gcp.cloudrunv2.ServiceTemplateArgs(
            service_account=web_sa.email,
            timeout='300s',
            max_instance_request_concurrency=10,
            scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(max_instance_count=100),
            containers=[
                gcp.cloudrunv2.ServiceTemplateContainerArgs(
                    name=spec.container_name,
                    image=f'{REGION}-docker.pkg.dev/{PROJECT}/images/web:{WEB_IMAGE_TAG}',
                    envs=[
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='BUCKET_SUFFIX', value=spec_name
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='IAP_EXPECTED_AUDIENCE',
                            value=f'/projects/{PROJECT_NUMBER}/global/backendServices/{spec.backend_service_id}',
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='MEMBERS_CACHE_LOCATION',
                            value=MEMBERS_CACHE_LOCATION,
                        ),
                    ],
                    ports=gcp.cloudrunv2.ServiceTemplateContainerPortsArgs(
                        container_port=8080, name='http1'
                    ),
                    resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                        limits={'cpu': spec.cpu, 'memory': spec.memory},
                        cpu_idle=True,
                    ),
                    startup_probe=gcp.cloudrunv2.ServiceTemplateContainerStartupProbeArgs(
                        failure_threshold=1,
                        period_seconds=240,
                        timeout_seconds=240,
                        tcp_socket=gcp.cloudrunv2.ServiceTemplateContainerStartupProbeTcpSocketArgs(
                            port=8080
                        ),
                    ),
                ),
            ],
        ),
        traffics=[
            gcp.cloudrunv2.ServiceTrafficArgs(
                type='TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST', percent=100
            ),
        ],
        opts=pulumi.ResourceOptions(
            protect=True,
            ignore_changes=[
                # Written by gcloud on every deploy
                'client',
                'clientVersion',
            ],
        ),
    )

    neg = gcp.compute.RegionNetworkEndpointGroup(
        f'{spec_name}-neg',
        name=f'{spec_name}-neg',
        region=REGION,
        project=PROJECT,
        network_endpoint_type='SERVERLESS',
        cloud_run=gcp.compute.RegionNetworkEndpointGroupCloudRunArgs(
            service=service.name
        ),
        opts=pulumi.ResourceOptions(protect=True),
    )

    backend = gcp.compute.BackendService(
        f'{spec_name}-backend',
        name=f'{spec_name}-backend',
        protocol='HTTP',
        port_name='http',
        load_balancing_scheme='EXTERNAL',
        timeout_sec=30,
        session_affinity='NONE',
        affinity_cookie_ttl_sec=0,
        connection_draining_timeout_sec=0,
        backends=[
            gcp.compute.BackendServiceBackendArgs(group=neg.id, capacity_scaler=0),
        ],
        # IAP uses the existing OAuth client `spec.oauth2_client_id`. The provider stores the client ID as
        # sensitive and the secret can't be read back, so both are left unmanaged.
        iap=gcp.compute.BackendServiceIapArgs(enabled=True),
        log_config=gcp.compute.BackendServiceLogConfigArgs(
            enable=True,
            optional_mode='EXCLUDE_ALL_OPTIONAL',
        ),
        opts=pulumi.ResourceOptions(
            protect=True,
            ignore_changes=[
                'iap.oauth2ClientId',
                'iap.oauth2ClientSecret',
                'iap.oauth2ClientSecretSha256',
            ],
        ),
    )

    for member in IAP_ACCESSORS:
        gcp.iap.WebBackendServiceIamMember(
            f'{spec_name}-backend-iap-{member.replace(":", "-").replace(".", "-")}',
            project=PROJECT,
            web_backend_service=backend.name,
            role='roles/iap.httpsResourceAccessor',
            member=member,
            opts=pulumi.ResourceOptions(protect=True),
        )

    https_url_map = gcp.compute.URLMap(
        f'{spec_name}-lb',
        name=f'{spec_name}-lb',
        project=PROJECT,
        default_service=backend.id,
        opts=pulumi.ResourceOptions(protect=True),
    )

    redirect_url_map = gcp.compute.URLMap(
        f'{spec_name}-http-redirect',
        name=f'{spec_name}-http-redirect',
        project=PROJECT,
        default_url_redirect=gcp.compute.URLMapDefaultUrlRedirectArgs(
            https_redirect=True,
            redirect_response_code='MOVED_PERMANENTLY_DEFAULT',
            strip_query=False,
        ),
        opts=pulumi.ResourceOptions(protect=True),
    )

    cert = gcp.compute.ManagedSslCertificate(
        f'{spec_name}-cert',
        name=f'{spec_name}-cert',
        project=PROJECT,
        managed=gcp.compute.ManagedSslCertificateManagedArgs(domains=[spec.domain]),
        opts=pulumi.ResourceOptions(protect=True),
    )

    https_proxy = gcp.compute.TargetHttpsProxy(
        f'{spec_name}-lb-target-proxy',
        name=f'{spec_name}-lb-target-proxy',
        project=PROJECT,
        url_map=https_url_map.id,
        ssl_certificates=[cert.id],
        ssl_policy=tls_policy.id,
        quic_override='NONE',
        tls_early_data='DISABLED',
        opts=pulumi.ResourceOptions(protect=True),
    )
    http_proxy = gcp.compute.TargetHttpProxy(
        f'{spec_name}-http-redirect-target-proxy',
        name=f'{spec_name}-http-redirect-target-proxy',
        project=PROJECT,
        url_map=redirect_url_map.id,
        opts=pulumi.ResourceOptions(protect=True),
    )

    ip = gcp.compute.GlobalAddress(
        spec.ip_name,
        name=spec.ip_name,
        project=PROJECT,
        address=spec.ip_address,
        ip_version='IPV4',
        opts=pulumi.ResourceOptions(protect=True),
    )

    https_rule = gcp.compute.GlobalForwardingRule(
        f'{spec_name}-frontend',
        name=f'{spec_name}-frontend',
        project=PROJECT,
        ip_address=ip.address,
        ip_protocol='TCP',
        port_range='443-443',
        network_tier='PREMIUM',
        target=https_proxy.id,
        opts=pulumi.ResourceOptions(protect=True),
    )
    http_rule = gcp.compute.GlobalForwardingRule(
        f'{spec_name}-http-redirect-frontend',
        name=f'{spec_name}-http-redirect-frontend',
        ip_address=ip.address,
        ip_protocol='TCP',
        port_range='80-80',
        network_tier='PREMIUM',
        target=http_proxy.id,
        opts=pulumi.ResourceOptions(protect=True),
    )

    return {
        spec_name: service,
        f'{spec_name}_backend': backend,
        f'{spec_name}_ip': ip,
        f'{spec_name}_https_rule': https_rule,
        f'{spec_name}_http_rule': http_rule,
    }
