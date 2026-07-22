package grafana

import "fmt"

// datasourceUID/dashboardUID are pinned, deterministic identifiers this
// package writes into its own provisioning files, so Probe/waitReady can
// address them directly via Grafana's API (/api/datasources/uid/<uid>,
// /api/dashboards/uid/<uid>) instead of searching by name.
const (
	datasourceUID = "prometheus"
	dashboardUID  = "datascape-overview"
)

// Provisioning file paths (ContainerSpec.Files) — Grafana's own file-based
// provisioning mechanism (docs.grafana.com "Provision Grafana"), not a
// database/API call this provider makes itself.
const (
	datasourceProvisioningPath        = "/etc/grafana/provisioning/datasources/prometheus.yaml"
	dashboardProviderProvisioningPath = "/etc/grafana/provisioning/dashboards/provider.yaml"
	dashboardJSONPath                 = "/etc/grafana/provisioning/dashboards-json/overview.json"
	dashboardsJSONDir                 = "/etc/grafana/provisioning/dashboards-json"
)

// datasourceYAML renders the Prometheus datasource provisioning file.
// prometheusInternal is req.PrometheusURL — an already-published,
// already-resolved endpoint fact (ADR 015: this package never constructs
// the address itself, only formats what the engine resolved from the
// prometheus Provider's own "prometheus" endpoint fact).
func datasourceYAML(prometheusInternal string) []byte {
	return []byte(fmt.Sprintf(`apiVersion: 1
datasources:
  - name: Prometheus
    uid: %s
    type: prometheus
    access: proxy
    url: %s
    isDefault: true
    editable: false
`, datasourceUID, prometheusInternal))
}

// dashboardProviderYAML is static: it names the on-disk folder
// (dashboardsJSONDir) Grafana scans for dashboard JSON files, never a
// per-deployment value.
var dashboardProviderYAML = []byte(fmt.Sprintf(`apiVersion: 1
providers:
  - name: datascape
    orgId: 1
    folder: ''
    type: file
    disableDeletion: true
    editable: false
    options:
      path: %s
`, dashboardsJSONDir))

// starterDashboardJSON is the minimal broker + database overview dashboard
// (docs/planning/08 C9 completion's Accept criterion: "at least one
// starter dashboard JSON ... keep it minimal and legible") — three stat
// panels reading metric names common across the exporters this slice
// ships (redpanda/minio's own "up", postgres_exporter's pg_up,
// mysqld_exporter's mysql_up), deliberately not tied to any one
// deployment's exact job names.
var starterDashboardJSON = []byte(fmt.Sprintf(`{
  "uid": "%s",
  "title": "Datascape Overview",
  "editable": false,
  "schemaVersion": 39,
  "version": 1,
  "panels": [
    {
      "id": 1,
      "type": "stat",
      "title": "Scrape Targets Up",
      "gridPos": {"h": 6, "w": 8, "x": 0, "y": 0},
      "targets": [{"expr": "sum(up)", "refId": "A"}]
    },
    {
      "id": 2,
      "type": "stat",
      "title": "Postgres Up",
      "gridPos": {"h": 6, "w": 8, "x": 8, "y": 0},
      "targets": [{"expr": "sum(pg_up)", "refId": "A"}]
    },
    {
      "id": 3,
      "type": "stat",
      "title": "MySQL Up",
      "gridPos": {"h": 6, "w": 8, "x": 16, "y": 0},
      "targets": [{"expr": "sum(mysql_up)", "refId": "A"}]
    }
  ]
}
`, dashboardUID))
