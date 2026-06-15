# Demo 22 — Pulumi Provider

**Module brief:** [§Module 22](../../../docs/website-brief-v0.4.0.md)

## UVP

> Same five resources as the Terraform provider, surfaced as
> first-class Pulumi types in TypeScript / Python / Go / .NET.
> Built via `pulumi-terraform-bridge` so it tracks the Terraform
> schema for free.

## Use cases

- **Pulumi shops.** Avoid forcing infrastructure-as-code rewrites;
  HeliosProxy plugs into your existing Pulumi pipeline.
- **TypeScript / Python infrastructure.** Type-checked HCL beats
  string-typed YAML for catching errors at the right time.
- **Inline computation.** Pulumi lets you compute resource values
  in your language — useful when node IPs come from a separate
  cloud-resource lookup.

## What this demo shows

The same triangle as Demo 21 (PoolProfile + AuditPolicy +
RoutingRule + TenantQuota + HeliosProxy) declared in TypeScript:

```ts
import * as heliosproxy from "@pulumi/heliosproxy";

const pool = new heliosproxy.PoolProfile("default", {
  name: "default-pool",
  mode: "transaction",
  maxPoolSize: 200,
});

const audit = new heliosproxy.AuditPolicy("pci", {
  name: "pci-audit",
  hashChain: true,
  retentionDays: 2555,
  backend: { type: "s3", bucket: "acme-pci-audit", region: "us-east-1" },
});

export const analytics = new heliosproxy.Instance("analytics", {
  name: "analytics",
  replicas: 2,
  image: "ghcr.io/heliosdatabase/hdb-heliosdb-proxy:0.6.1",
  nodes: [
    { host: "pg-primary.db.svc",   port: 5432, role: "primary", weight: 100 },
    { host: "pg-standby.db.svc",   port: 5432, role: "standby", weight: 100 },
  ],
  poolProfileRef: pool.name,
  auditPolicyRef: audit.name,
});
```

## Run it

```bash
# Prereq: kind cluster with operator running (Demo 20)

# 1. Build the Pulumi provider + SDKs
cd ../../../../pulumi-HDB-HeliosDB-Proxy
make build

# 2. Initialise the demo program
cd /path/to/demos/v0.4.0/22-pulumi
npm install
pulumi stack init dev
pulumi up
```

Expected:

```text
Updating (dev):
  + pulumi:pulumi:Stack         hdb-demo-22-pulumi-dev create
  + heliosproxy:index:PoolProfile default                      create
  + heliosproxy:index:AuditPolicy pci                          create
  + heliosproxy:index:Instance    analytics                    create

Outputs:
  + analytics: { name: "analytics", currentPrimary: "pg-primary.db.svc:5432" }

Resources:
  + 4 created

Duration: 32s
```

## Implementation pointer

- Provider entry: `pulumi-HDB-HeliosDB-Proxy/provider/cmd/pulumi-resource-heliosproxy/main.go`
- Bridge config: `provider/resources.go::Provider()` (token mappings + SDK packages)
- TypeScript example: `examples/typescript/index.ts`

## HeliosDB compatibility

Same as Demo 21 — provider talks to the operator; backend-agnostic.
