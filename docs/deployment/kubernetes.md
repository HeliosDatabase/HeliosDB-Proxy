# Kubernetes Deployment

This guide covers deploying HeliosProxy on Kubernetes, including the Deployment pattern, sidecar pattern, ConfigMap, Service, health probes, and Prometheus ServiceMonitor.

---

## Deployment Patterns

HeliosProxy supports two Kubernetes deployment patterns:

| Pattern | Description | Best For |
|---------|-------------|----------|
| **Deployment** | Dedicated proxy pods behind a Service. Applications connect via the Service. | Shared proxy for multiple applications, centralized management. |
| **Sidecar** | Proxy container co-located in each application Pod. Applications connect via `localhost`. | Lowest latency, per-application isolation. |

---

## ConfigMap

The proxy configuration is stored in a ConfigMap and mounted as a volume into the proxy container.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: heliosproxy-config
  namespace: default
  labels:
    app.kubernetes.io/name: heliosproxy
    app.kubernetes.io/component: config
data:
  config.toml: |
    listen_address = "0.0.0.0:6432"
    admin_address = "0.0.0.0:9090"
    tr_enabled = true
    tr_mode = "session"
    write_timeout_secs = 30

    [pool_mode]
    mode = "transaction"
    max_pool_size = 100
    min_idle = 10
    idle_timeout_secs = 600
    max_lifetime_secs = 3600
    acquire_timeout_secs = 5
    reset_query = "DISCARD ALL"
    prepared_statement_mode = "track"

    [pool]
    min_connections = 5
    max_connections = 100
    idle_timeout_secs = 300
    max_lifetime_secs = 1800
    acquire_timeout_secs = 30
    test_on_acquire = true

    [load_balancer]
    read_strategy = "least_connections"
    read_write_split = true
    latency_threshold_ms = 50

    [health]
    check_interval_secs = 5
    check_timeout_secs = 3
    failure_threshold = 3
    success_threshold = 2
    check_query = "SELECT 1"

    [[nodes]]
    host = "heliosdb-primary.default.svc.cluster.local"
    port = 5432
    http_port = 8080
    role = "primary"
    weight = 100
    name = "primary"

    [[nodes]]
    host = "heliosdb-standby-0.default.svc.cluster.local"
    port = 5432
    http_port = 8080
    role = "standby"
    weight = 100
    name = "standby-0"

    [[nodes]]
    host = "heliosdb-standby-1.default.svc.cluster.local"
    port = 5432
    http_port = 8080
    role = "standby"
    weight = 100
    name = "standby-1"
```

---

## Deployment Pattern

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: heliosproxy
  namespace: default
  labels:
    app.kubernetes.io/name: heliosproxy
    app.kubernetes.io/component: proxy
spec:
  replicas: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: heliosproxy
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 1
  template:
    metadata:
      labels:
        app.kubernetes.io/name: heliosproxy
        app.kubernetes.io/component: proxy
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics/prometheus"
    spec:
      serviceAccountName: heliosproxy
      securityContext:
        runAsNonRoot: true
        runAsUser: 65534
        fsGroup: 65534
      containers:
        - name: heliosproxy
          image: heliosdb/proxy:latest
          args:
            - "--config"
            - "/etc/heliosproxy/config.toml"
          ports:
            - name: postgres
              containerPort: 6432
              protocol: TCP
            - name: admin
              containerPort: 9090
              protocol: TCP
          env:
            - name: RUST_LOG
              value: "heliosdb_proxy=info"
          volumeMounts:
            - name: config
              mountPath: /etc/heliosproxy
              readOnly: true
          resources:
            requests:
              cpu: 250m
              memory: 128Mi
            limits:
              cpu: "2"
              memory: 512Mi
          livenessProbe:
            httpGet:
              path: /health/live
              port: admin
            initialDelaySeconds: 5
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /health/ready
              port: admin
            initialDelaySeconds: 2
            periodSeconds: 5
            timeoutSeconds: 3
            failureThreshold: 3
          startupProbe:
            httpGet:
              path: /health
              port: admin
            initialDelaySeconds: 1
            periodSeconds: 2
            timeoutSeconds: 3
            failureThreshold: 15
      volumes:
        - name: config
          configMap:
            name: heliosproxy-config
      terminationGracePeriodSeconds: 30
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: heliosproxy
  namespace: default
  labels:
    app.kubernetes.io/name: heliosproxy
    app.kubernetes.io/component: proxy
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: heliosproxy
  ports:
    - name: postgres
      port: 6432
      targetPort: postgres
      protocol: TCP
    - name: admin
      port: 9090
      targetPort: admin
      protocol: TCP
```

### ServiceAccount

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: heliosproxy
  namespace: default
  labels:
    app.kubernetes.io/name: heliosproxy
```

### Application Connection

Applications connect to the proxy via the Kubernetes Service DNS name:

```yaml
# In your application Deployment
env:
  - name: DATABASE_URL
    value: "postgres://appuser:password@heliosproxy.default.svc.cluster.local:6432/appdb"
```

---

## Sidecar Pattern

Deploy HeliosProxy as a sidecar container within each application Pod. The application connects to `localhost:6432`, eliminating network hops.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
  namespace: default
spec:
  replicas: 3
  selector:
    matchLabels:
      app: myapp
  template:
    metadata:
      labels:
        app: myapp
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics/prometheus"
    spec:
      containers:
        # ── Application Container ─────────────────────────────
        - name: app
          image: myapp:latest
          ports:
            - containerPort: 8080
          env:
            - name: DATABASE_URL
              value: "postgres://appuser:password@localhost:6432/appdb"
          resources:
            requests:
              cpu: 500m
              memory: 256Mi

        # ── HeliosProxy Sidecar ───────────────────────────────
        - name: heliosproxy
          image: heliosdb/proxy:latest
          args:
            - "--config"
            - "/etc/heliosproxy/config.toml"
          ports:
            - name: postgres
              containerPort: 6432
            - name: admin
              containerPort: 9090
          env:
            - name: RUST_LOG
              value: "heliosdb_proxy=info"
          volumeMounts:
            - name: proxy-config
              mountPath: /etc/heliosproxy
              readOnly: true
          resources:
            requests:
              cpu: 100m
              memory: 64Mi
            limits:
              cpu: 500m
              memory: 256Mi
          livenessProbe:
            httpGet:
              path: /health/live
              port: admin
            initialDelaySeconds: 3
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health/ready
              port: admin
            initialDelaySeconds: 2
            periodSeconds: 5

      volumes:
        - name: proxy-config
          configMap:
            name: heliosproxy-config
```

### Sidecar Advantages

- Zero network latency between application and proxy (localhost).
- Per-application connection pool isolation.
- Application and proxy scale together.
- No shared proxy bottleneck.

### Sidecar Considerations

- Each Pod runs its own proxy instance, increasing resource usage.
- Configuration changes require a Pod restart (unless using a ConfigMap watcher).
- Connection pool sizes should be smaller (per-Pod rather than shared).

---

## Health Probes

HeliosProxy provides three health endpoints for Kubernetes probes:

| Probe | Endpoint | Purpose |
|-------|----------|---------|
| Liveness | `GET /health/live` | Restart the Pod if the proxy process is unresponsive. |
| Readiness | `GET /health/ready` | Remove from Service endpoints if no healthy backends are available. |
| Startup | `GET /health` | Allow extra time for initial backend connection establishment. |

### Probe Configuration

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /health/ready
    port: 9090
  initialDelaySeconds: 2
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3

startupProbe:
  httpGet:
    path: /health
    port: 9090
  initialDelaySeconds: 1
  periodSeconds: 2
  timeoutSeconds: 3
  failureThreshold: 15
```

The startup probe allows up to 30 seconds (15 failures at 2-second intervals) for the proxy to establish backend connections before the liveness probe begins.

---

## Prometheus ServiceMonitor

For Prometheus Operator-based monitoring, create a ServiceMonitor to scrape proxy metrics.

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: heliosproxy
  namespace: default
  labels:
    app.kubernetes.io/name: heliosproxy
    release: prometheus
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: heliosproxy
  endpoints:
    - port: admin
      path: /metrics/prometheus
      interval: 15s
      scrapeTimeout: 10s
```

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `heliosdb_proxy_connections_total` | Counter | Total client connections accepted. |
| `heliosdb_proxy_connections_closed` | Counter | Total client connections closed. |
| `heliosdb_proxy_queries_total` | Counter | Total queries processed. |
| `heliosdb_proxy_bytes_received_total` | Counter | Total bytes received from clients. |
| `heliosdb_proxy_bytes_sent_total` | Counter | Total bytes sent to clients. |
| `heliosdb_proxy_failovers_total` | Counter | Total failover events. |

### Grafana Dashboard

A sample Grafana dashboard JSON can be imported from the project repository. Key panels include:

- Active connections over time
- Query throughput (read vs. write)
- Connection pool utilization per node
- Failover event markers
- Node health status

---

## Pod Disruption Budget

Ensure at least one proxy instance remains available during node maintenance or cluster upgrades.

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: heliosproxy-pdb
  namespace: default
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: heliosproxy
```

---

## Horizontal Pod Autoscaler

Scale the proxy deployment based on CPU utilization.

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: heliosproxy-hpa
  namespace: default
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: heliosproxy
  minReplicas: 2
  maxReplicas: 8
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

---

## Network Policy

Restrict network access to the proxy:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: heliosproxy-network-policy
  namespace: default
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: heliosproxy
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Allow PostgreSQL connections from application Pods
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/part-of: myapp
      ports:
        - port: 6432
          protocol: TCP
    # Allow admin/metrics access from monitoring namespace
    - from:
        - namespaceSelector:
            matchLabels:
              name: monitoring
      ports:
        - port: 9090
          protocol: TCP
  egress:
    # Allow connections to database backends
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: heliosdb
      ports:
        - port: 5432
          protocol: TCP
        - port: 8080
          protocol: TCP
    # Allow DNS resolution
    - to: []
      ports:
        - port: 53
          protocol: UDP
        - port: 53
          protocol: TCP
```

---

## TLS with cert-manager

Generate TLS certificates for client-facing connections using cert-manager.

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: heliosproxy-tls
  namespace: default
spec:
  secretName: heliosproxy-tls-secret
  issuerRef:
    name: letsencrypt-prod
    kind: ClusterIssuer
  dnsNames:
    - heliosproxy.default.svc.cluster.local
    - heliosproxy.example.com
```

Mount the TLS secret into the proxy container and reference it in the configuration:

```yaml
# In the Deployment spec
volumeMounts:
  - name: tls-certs
    mountPath: /etc/heliosproxy/tls
    readOnly: true
volumes:
  - name: tls-certs
    secret:
      secretName: heliosproxy-tls-secret
```

Update `config.toml`:

```toml
[tls]
enabled = true
cert_path = "/etc/heliosproxy/tls/tls.crt"
key_path = "/etc/heliosproxy/tls/tls.key"
```

---

## Complete Kustomization

For managing all resources together with Kustomize:

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: default

resources:
  - serviceaccount.yaml
  - configmap.yaml
  - deployment.yaml
  - service.yaml
  - pdb.yaml
  - hpa.yaml
  - servicemonitor.yaml
  - networkpolicy.yaml

commonLabels:
  app.kubernetes.io/name: heliosproxy
  app.kubernetes.io/version: "0.3.0"
  app.kubernetes.io/managed-by: kustomize
```

Deploy:

```bash
kubectl apply -k .
```

---

## Troubleshooting

### Proxy Pod in CrashLoopBackOff

```bash
# Check container logs
kubectl logs deployment/heliosproxy -c heliosproxy

# Common causes:
# - Invalid configuration file (TOML parse error)
# - Backend nodes unreachable (DNS resolution failure)
# - Port conflict with another container
```

### Readiness Probe Failing

```bash
# Check if backends are reachable from the proxy Pod
kubectl exec deployment/heliosproxy -- curl -s http://localhost:9090/nodes

# Verify backend DNS resolution
kubectl exec deployment/heliosproxy -- nslookup heliosdb-primary.default.svc.cluster.local
```

### Connection Timeouts

```bash
# Check proxy metrics for pool exhaustion
kubectl exec deployment/heliosproxy -- curl -s http://localhost:9090/pools

# Increase pool size or switch to transaction pooling mode
# Edit the ConfigMap and restart the Pods
kubectl rollout restart deployment/heliosproxy
```

---

## See Also

- [Standalone Deployment](standalone.md)
- [Docker Deployment](docker.md)
- [Configuration Reference](../configuration.md)
- [Admin API Reference](../admin-api.md)
