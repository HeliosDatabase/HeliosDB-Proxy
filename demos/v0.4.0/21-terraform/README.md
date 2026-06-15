# Demo 21 — Terraform Provider

**Module brief:** [§Module 21](../../../docs/website-brief-v0.4.0.md)

## UVP

> Declare HeliosProxy from `main.tf`. Five resources mirror the
> operator CRDs 1:1; schema imported directly from the operator's
> Go types so it never drifts.

## Use cases

- **GitOps Terraform shops.** Plug HeliosProxy into your existing
  Terraform pipeline; no operational handoff to a separate tool.
- **Multi-cloud.** Single `main.tf` declares HeliosProxy + EKS
  + RDS; one `terraform apply` brings up the full stack.
- **Drift detection.** `terraform plan` shows when someone edited
  the CR by hand outside Terraform.

## What this demo shows

A working `main.tf` against the operator's CRDs (Demo 20 sets up
the cluster). One `terraform apply` brings up the full triangle:
PoolProfile + AuditPolicy + RoutingRule + TenantQuota +
HeliosProxy.

## Run it

```bash
# Prereq: kind cluster with operator running (Demo 20)

cd demos/v0.4.0/21-terraform

# 1. Build + install the provider (dev override)
cd ../../../../terraform-provider-HDB-HeliosDB-Proxy
make install

# 2. Configure provider for dev mode
cat <<EOF >> ~/.terraformrc
provider_installation {
  dev_overrides {
    "heliosdatabase/heliosproxy" = "$(go env GOBIN)"
  }
  direct {}
}
EOF

# 3. Apply
cd /path/to/demos/v0.4.0/21-terraform
terraform apply
```

`main.tf`:

```hcl
terraform {
  required_providers {
    heliosproxy = {
      source  = "heliosdatabase/heliosproxy"
      version = "~> 0.1"
    }
  }
}

provider "heliosproxy" {
  namespace = "data"
}

resource "heliosproxy_pool_profile" "default" {
  name = "default-pool"
  mode = "transaction"
  max_pool_size = 200
}

resource "heliosproxy_audit_policy" "pci" {
  name           = "pci-audit"
  hash_chain     = true
  retention_days = 2555
  backend = {
    type   = "s3"
    bucket = "acme-pci-audit"
    region = "us-east-1"
  }
  included_tables = ["payments", "cards"]
}

resource "heliosproxy_instance" "analytics" {
  name     = "analytics"
  replicas = 2
  image    = "ghcr.io/heliosdatabase/hdb-heliosdb-proxy:0.6.1"

  nodes = [
    { host = "pg-primary.db.svc",   port = 5432, role = "primary", weight = 100 },
    { host = "pg-standby.db.svc",   port = 5432, role = "standby", weight = 100 },
  ]

  pool = {
    min_connections      = 5
    max_connections      = 100
    idle_timeout_seconds = 300
  }

  pool_profile_ref = heliosproxy_pool_profile.default.name
  audit_policy_ref = heliosproxy_audit_policy.pci.name
}

output "current_primary" {
  value = heliosproxy_instance.analytics.current_primary
}
```

After `apply`:

```bash
terraform output current_primary
#  "pg-primary.db.svc:5432"
```

## Implementation pointer

- Provider entry: `terraform-provider-HDB-HeliosDB-Proxy/main.go`
- Resources: `internal/provider/*_resource.go` (one per CRD)
- Schema imported via local `replace` of the operator's
  `api/v1alpha1` package (see `go.mod`).

## HeliosDB compatibility

Provider talks to the operator; the operator is backend-agnostic
(Demo 20). Swap the `nodes:` host entries for HeliosDB endpoints.
