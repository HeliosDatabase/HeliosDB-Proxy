# Demo 20 — Kubernetes Operator

**Module brief:** [§Module 20](../../../docs/website-brief-v0.4.0.md)

## UVP

> One CR, one full HeliosProxy stack. Apply a `HeliosProxy`
> resource and the operator renders a ConfigMap, Deployment, and
> Service — owned objects auto-clean on `kubectl delete`.

## Use cases

- **Multi-environment GitOps.** Same CR YAML in dev / staging /
  prod; only the `replicas` and node list change.
- **Multi-tenant Kubernetes.** Each tenant's namespace owns a
  HeliosProxy CR with their own pool / routing / audit / quota
  refs.
- **Self-service DBA tooling.** Devs apply CRs; the operator
  enforces config schema and surfaces ref-missing conditions.

## What this demo shows

Spins up a `kind` cluster, installs the operator's CRDs,
applies a `HeliosProxy` CR + its dependent resources, watches the
status fields populate.

## Run it

```bash
cd demos/v0.4.0/20-k8s-operator
./demo.sh
```

Sequence:

```bash
# 1. Create kind cluster
kind create cluster --name hdb-demo

# 2. Install operator CRDs
kubectl apply -f \
  ../../../../HDB-HeliosDB-Proxy-Operator/config/crd/bases/

# 3. Run operator (out-of-cluster for demo speed)
cd ../../../../HDB-HeliosDB-Proxy-Operator
go run ./cmd --metrics-bind-address=:8080 --leader-elect=false &

# 4. Apply sample CR
kubectl apply -f config/samples/heliosproxy_v1alpha1_heliosproxy.yaml

# 5. Watch reconcile populate ConfigMap + Deployment + Service
kubectl get heliosproxy analytics -n data -w
#  NAME       REPLICAS  PHASE     PRIMARY                AGE
#  analytics  2         Pending                          5s
#  analytics  2         Degraded  pg-primary.db.svc:5432 12s
#  analytics  2         Ready     pg-primary.db.svc:5432 25s

# 6. Confirm owned objects
kubectl get cm,deploy,svc -n data -l app.kubernetes.io/instance=analytics
#  NAME                       DATA   AGE
#  configmap/analytics-config 1      30s
#  
#  NAME                            READY   UP-TO-DATE   AVAILABLE
#  deployment.apps/analytics       2/2     2            2
#  
#  NAME                TYPE        CLUSTER-IP    EXTERNAL-IP   PORT(S)
#  service/analytics   ClusterIP   10.96.0.42    <none>        5432/TCP,9090/TCP

# 7. Delete the CR — owned objects cascade
kubectl delete heliosproxy analytics -n data
kubectl get cm,deploy,svc -n data -l app.kubernetes.io/instance=analytics
#  No resources found.
```

## Implementation pointer

- CRDs: `HDB-HeliosDB-Proxy-Operator/api/v1alpha1/*.go`
- Reconciler: `internal/controller/heliosproxy_controller.go`
- Render helpers: `internal/controller/render.go` (12 unit tests)
- Topology polling: `internal/controller/topology.go`

## HeliosDB compatibility

The operator is backend-agnostic — applies to PG and HeliosDB
identically. The sample CR uses `image: ghcr.io/heliosdatabase/hdb-heliosdb-proxy:0.6.0`;
swap the `nodes:` host entries for HeliosDB-Lite endpoints.
