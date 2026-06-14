# `terraform-provider-heliosproxy` — scaffold

Design for T1.3, the Terraform + Pulumi providers. Like the operator,
the full code lives in separate repos
(`terraform-provider-heliosproxy`, `pulumi-heliosproxy`) — this
directory captures the resource schema so the Rust repo and the Go
provider stay coherent on the CRDs.

Providers thin-wrap the T1.1 operator CRDs: apply against a
Kubernetes cluster where the operator is installed.

## Provider configuration

```hcl
terraform {
  required_providers {
    heliosproxy = {
      source  = "heliosdatabase/heliosproxy"
      version = "~> 0.3"
    }
  }
}

provider "heliosproxy" {
  # Uses KUBECONFIG by default; override to target a specific context.
  kube_context = "prod"
  namespace    = "data"
}
```

## Resources

### `heliosproxy_instance`

```hcl
resource "heliosproxy_instance" "analytics" {
  name     = "analytics"
  replicas = 2
  image    = "ghcr.io/heliosdatabase/hdb-heliosdb-proxy:0.4.0"

  node {
    host   = "pg-primary.db.svc"
    port   = 5432
    role   = "primary"
    weight = 100
  }

  node {
    host   = "pg-standby.db.svc"
    port   = 5432
    role   = "standby"
    weight = 100
  }

  pool {
    min_connections   = 5
    max_connections   = 100
    idle_timeout_secs = 300
  }

  features = {
    tr            = true
    pool_modes    = true
    rate_limiting = true
  }

  pool_profile_ref = heliosproxy_pool_profile.default.id
}
```

### `heliosproxy_pool_profile`

```hcl
resource "heliosproxy_pool_profile" "default" {
  name                     = "default-pool"
  mode                     = "transaction"
  max_pool_size            = 200
  min_idle                 = 20
  idle_timeout_secs        = 60
  max_lifetime_secs        = 1800
  prepared_statement_mode  = "track"
  reset_query              = "DISCARD ALL"
}
```

### `heliosproxy_routing_rule`

```hcl
resource "heliosproxy_routing_rule" "analytics_reads" {
  name = "analytics-reads-to-standby"

  match {
    application_name_patterns = ["analytics-*"]
    query_pattern             = "^SELECT .*FROM events"
  }

  route              = "standby"
  consistency        = "eventual"
  max_lag_millis     = 5000
}
```

### `heliosproxy_audit_policy`

```hcl
resource "heliosproxy_audit_policy" "pci" {
  name            = "pci-audit"
  hash_chain      = true
  retention_days  = 2555

  backend {
    type   = "s3"
    bucket = "acme-pci-audit"
    region = "us-east-1"
  }

  included_tables = ["payments", "cards"]
  excluded_users  = ["monitoring"]
}
```

### `heliosproxy_tenant_quota`

```hcl
resource "heliosproxy_tenant_quota" "free_tier" {
  name = "free-tier"

  max_concurrent_connections    = 10
  max_queries_per_minute        = 600
  max_bytes_read_per_minute     = 100000000
  cost_budget_dollars_per_day   = 5
}
```

## Data sources

- `data.heliosproxy_instance.<name>` — read-only snapshot of an
  existing instance's status (currentPrimary, healthyNodes, etc.) for
  use in downstream resources (e.g. outputs, conditional routing).

## Reference module

```hcl
module "helios_3node" {
  source  = "heliosdatabase/heliosproxy/heliosproxy"
  version = "~> 0.3"

  name     = "app-db"
  replicas = 2

  nodes = [
    { host = "pg-primary.db.svc",   role = "primary",  weight = 100 },
    { host = "pg-standby-a.db.svc", role = "standby",  weight = 100 },
    { host = "pg-standby-b.db.svc", role = "standby",  weight = 50  },
  ]

  audit_policy = {
    name      = "app-audit"
    backend   = "s3"
    bucket    = "acme-app-audit"
    region    = "us-east-1"
  }
}
```

`terraform apply` on this module stands up the full triangle (proxy
instance + 3 CRDs) and blocks on the operator reporting
`status.phase = Ready`.

## Acceptance (T1.3 exit)

- `terraform init && terraform apply` on the reference module stands
  up a working 3-node cluster in a kind/minikube test environment.
- `terraform destroy` returns the cluster to pristine state.
- `terraform import` works for each resource type (required for
  brownfield adoption).

## Pulumi equivalent

A Pulumi provider (`pulumi-heliosproxy`) exposes the same resources
with identical schemas, generated from the Terraform provider via
`pulumi-terraform-bridge`.

## Status

Design contract only. Go provider scaffolding (`terraform-plugin-framework` +
`go-operator-sdk` client-go) lives in a separate repo so Go doesn't
leak into the Rust build graph.
