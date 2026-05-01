---
name: heliosproxy-iac
description: Deploy HeliosProxy via Kubernetes operator (CRD), Terraform provider, or Pulumi. Quick-start per surface and which to pick when. Use when the user says "operator", "Kubernetes", "CRD", "HeliosProxy resource", "Terraform", "TF provider", "Pulumi", "IaC".
allowed-tools: Bash(kubectl *), Bash(helm *), Bash(terraform *), Bash(pulumi *), Read
related: [heliosproxy-overview, heliosproxy-install, heliosproxy-config]
---

# Infrastructure-as-Code surfaces

Three sibling repositories provide IaC entry points for HeliosProxy.
Pick the one that matches your existing platform stack.

🟠 Mutating — IaC tools change real cloud / cluster state.

## Pick a surface

| Surface | Best for | Sibling repo |
|---|---|---|
| **Kubernetes operator** + `HeliosProxy` CRD | Anyone running on k8s | [`HDB-HeliosDB-Proxy-Operator`](https://github.com/dimensigon/HDB-HeliosDB-Proxy-Operator) |
| **Terraform provider**                       | Multi-cloud, IaC pipelines | [`terraform-provider-HDB-HeliosDB-Proxy`](https://github.com/dimensigon/terraform-provider-HDB-HeliosDB-Proxy) |
| **Pulumi provider**                          | TypeScript / Python IaC | [`pulumi-HDB-HeliosDB-Proxy`](https://github.com/dimensigon/pulumi-HDB-HeliosDB-Proxy) |

Operator and Terraform/Pulumi compose: many teams use the operator
for k8s-native lifecycle and Terraform/Pulumi to declare the CR
itself.

## Recipes

### Recipe 1: Kubernetes operator quick-start

```bash
# 1. Install CRDs + operator (Helm chart)
helm repo add heliosproxy https://dimensigon.github.io/HDB-HeliosDB-Proxy-Operator
helm repo update
helm install heliosproxy heliosproxy/heliosproxy-operator \
  --namespace heliosproxy --create-namespace
```

Wait for the operator pod to be Ready, then declare a `HeliosProxy`:

```yaml
# heliosproxy.yaml
apiVersion: heliosproxy.dimensigon.io/v1
kind: HeliosProxy
metadata:
  name: prod-proxy
  namespace: heliosproxy
spec:
  image: ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.1
  replicas: 2
  features: ["pool-modes", "ha-tr", "wasm-plugins", "anomaly-detection"]
  nodes:
    - address: "pg-primary.db:5432"
      role:    primary
      weight:  1
    - address: "pg-replica-1.db:5432"
      role:    standby
      weight:  1
  pool:
    maxConnections: 200
  ha:
    autoFailover: true
  service:
    type: ClusterIP
    pgPort:    6432
    adminPort: 9090
```

```bash
kubectl apply -f heliosproxy.yaml
kubectl -n heliosproxy get heliosproxy prod-proxy -w
# NAME         READY   AGE
# prod-proxy   2/2     45s
```

The operator reconciles into:

- ConfigMap (`prod-proxy-config`) holding the rendered `proxy.toml`
- Deployment (`prod-proxy`) with `replicas: 2`
- Service (`prod-proxy`) exposing PG and admin ports
- (optionally) PodDisruptionBudget, ServiceMonitor for Prometheus

`status.currentPrimary`, `status.healthyNodes`, etc. mirror
`/topology` exactly — the operator polls the proxy and patches
status.

### Recipe 2: Terraform provider

```hcl
terraform {
  required_providers {
    heliosproxy = {
      source  = "dimensigon/heliosproxy"
      version = "~> 0.4"
    }
  }
}

provider "heliosproxy" {
  kubeconfig = "~/.kube/config"
  namespace  = "heliosproxy"
}

resource "heliosproxy_instance" "prod" {
  name      = "prod-proxy"
  image     = "ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.1"
  replicas  = 2
  features  = ["pool-modes", "ha-tr"]

  node {
    address = "pg-primary.db:5432"
    role    = "primary"
    weight  = 1
  }
  node {
    address = "pg-replica-1.db:5432"
    role    = "standby"
    weight  = 1
  }
}

resource "heliosproxy_routing_rule" "vector_traffic" {
  instance = heliosproxy_instance.prod.name
  match    = "embedding <=> '"
  target   = "pg-vector:5432"
}
```

```bash
terraform init
terraform plan
terraform apply
```

Under the hood, the provider talks to k8s and creates the same CR
the operator reconciles. State lives in your tfstate, the operator
owns reconciliation.

### Recipe 3: Pulumi (TypeScript)

```ts
import * as helios from "@dimensigon/pulumi-heliosproxy";

const prod = new helios.Instance("prod-proxy", {
  image:    "ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.1",
  replicas: 2,
  features: ["pool-modes", "ha-tr"],
  nodes: [
    { address: "pg-primary.db:5432",   role: "primary", weight: 1 },
    { address: "pg-replica-1.db:5432", role: "standby", weight: 1 },
  ],
});

new helios.RoutingRule("vector-traffic", {
  instance: prod.name,
  match:    "embedding <=> '",
  target:   "pg-vector:5432",
});
```

```bash
pulumi up
```

Same model as Terraform — the provider is a thin shim over the
k8s CRD.

### Recipe 4: Confirm the proxy actually deployed

```bash
kubectl -n heliosproxy get all -l app.kubernetes.io/name=heliosproxy
kubectl -n heliosproxy logs -l app.kubernetes.io/name=heliosproxy -f

# port-forward to test admin endpoints from your laptop
kubectl -n heliosproxy port-forward svc/prod-proxy 9090:9090
curl -s http://localhost:9090/topology | jq .
```

### Recipe 5: Roll out a new version

With the operator (k8s-native rollout):

```bash
kubectl -n heliosproxy patch heliosproxy prod-proxy \
  --type merge \
  -p '{"spec":{"image":"ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.2"}}'
```

The operator handles the rolling update with respect for
`PodDisruptionBudget` and the proxy's drain semantics
(`heliosproxy-shutdown`).

With Terraform:

```hcl
resource "heliosproxy_instance" "prod" {
  image = "ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.2"
}
```

```bash
terraform apply
```

The TF provider patches the CR; the operator reconciles.

## Pitfalls

- **The operator is the source of truth.** Don't hand-edit the
  Deployment / ConfigMap the operator creates — it'll be reverted
  on the next reconcile loop. Edit the `HeliosProxy` CR.
- **Image must match the features.** If the CR sets
  `features: ["wasm-plugins"]` but the image was built without
  that feature, plugin endpoints return 503 at runtime. The
  default `ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.1` image is
  built with `--features all-features`.
- **Operator version vs proxy version.** They don't have to match
  exactly, but the operator declares minimum + tested proxy
  versions in its README. Pinning the operator to `0.4` and the
  proxy image to `0.4.x` is the safe pattern.
- **Terraform / Pulumi state can drift** if someone hand-edits the
  CR via `kubectl edit`. The next `apply` overwrites those edits.
  Adopt one workflow: either CR is owned by IaC, or by the
  operator-only path.
- **Don't run two operators on the same cluster.** They'll race
  to reconcile.
- **`PodDisruptionBudget`** isn't enabled by default. For prod,
  set `spec.podDisruptionBudget: {minAvailable: 1}` so a node
  drain doesn't take both replicas down simultaneously.

## See also

- `heliosproxy-config` — what the rendered `proxy.toml` looks like
- `heliosproxy-install` — same image used by container deploys
- `heliosproxy-release` — how the image gets built and tagged
- Operator repo: <https://github.com/dimensigon/HDB-HeliosDB-Proxy-Operator>
- TF provider: <https://github.com/dimensigon/terraform-provider-HDB-HeliosDB-Proxy>
- Pulumi provider: <https://github.com/dimensigon/pulumi-HDB-HeliosDB-Proxy>
- Demos: [`demos/v0.4.0/{20-k8s-operator,21-terraform,22-pulumi}/`](../../demos/v0.4.0/)
