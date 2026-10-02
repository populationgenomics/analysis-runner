# Infrastructure (Pulumi)

Pulumi program for the analysis-runner GCP infrastructure: the **server** (`server`,
`server-test`), the **web proxies** (`main-web`, `test-web`, with their load balancers and IAP)
and the **metamist consumer** (`sample_metadata`).

The **stack config lives in the private `analysis-runner-private` repo**
(`infrastructure/Pulumi.<stack>.yaml`), the same split as `metamist` / `metamist-private`.
Environment-specific values (project, IPs, domains, OAuth client IDs, service account names,
sizing, etc.) are read from that config, so none of them are in this repo.
`.gitignore` excludes `infrastructure/Pulumi.*.yaml` so a copied stack file can't be committed.

| Module | Contents |
|---|---|
| `components/server.py` | Server service account, Cloud Run `server` / `server-test`, topic `submissions`, Artifact Registry `images`, bucket `cpg-config`, and the server's IAM on those |
| `components/web.py` | `web-server` service account, Cloud Run web proxies, serverless NEGs, backend services (IAP), URL maps, proxies, certs, IPs, forwarding rules, TLS policy, IAP access |
| `components/metamist_consumer.py` | `sample-metadata` service account and the gen1 Cloud Function (to be removed) |

## Not managed here
- **Secrets:** services only reference `server-config` by name.
- **Deploy identity:** the GitHub WIF pool and `server-deploy@`, and its grants.
- **cpg-infrastructure:** `run.invoker` for `<dataset>-analysis` groups, the `images` reader
  group, the `cpg-config` template objects and viewer group.
- **Images:** server / web images and `DRIVER_IMAGE` are still deployed by the
  `deploy_server.yaml` / `deploy_web_server.yaml` workflows, so Pulumi ignores them.

All existing resources were adopted with `pulumi import` and are protected (`protect=True`).

## Running it
```bash
cd infrastructure
uv sync                                   # installs pulumi + pulumi-gcp into infrastructure/.venv
cp ../../analysis-runner-private/infrastructure/Pulumi.production.yaml .
gcloud auth application-default login
pulumi login <state backend>
pulumi preview --diff --stack production  # must show no changes before any `pulumi up`
```