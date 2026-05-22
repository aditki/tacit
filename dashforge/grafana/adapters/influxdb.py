"""Adapter for InfluxDB datasource (InfluxQL and Flux)."""
from __future__ import annotations

import structlog

from dashforge.grafana.adapters.base import DatasourceAdapter
from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DatasourceInfo, MetricEntry

logger = structlog.get_logger()


class InfluxDBAdapter(DatasourceAdapter):

    @property
    def query_language(self) -> str:
        return "influxql"

    @property
    def supported_types(self) -> set[str]:
        return {"influxdb"}

    async def discover_metrics(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        keywords: list[str],
    ) -> list[MetricEntry]:
        entries: list[MetricEntry] = []
        is_flux = datasource.json_data.get("version") == "Flux"

        # InfluxQL: SHOW MEASUREMENTS
        try:
            data = await client.datasource_proxy_get(
                datasource.uid, "query?q=SHOW+MEASUREMENTS&db=" + datasource.json_data.get("database", "")
            )
            measurements: list[str] = []
            if isinstance(data, dict):
                for result in data.get("results", []):
                    for series in result.get("series", []):
                        for val in series.get("values", []):
                            if val:
                                measurements.append(val[0])
        except Exception:
            logger.warning("influxdb_measurements_failed", datasource=datasource.name)
            return []

        kw_lower = [k.lower() for k in keywords]
        for meas in measurements:
            if kw_lower and not any(k in meas.lower() for k in kw_lower):
                continue
            entries.append(
                MetricEntry(
                    name=meas,
                    datasource_uid=datasource.uid,
                    datasource_name=datasource.name,
                    datasource_type=datasource.type,
                    query_language="flux" if is_flux else self.query_language,
                )
            )

        if not entries:
            for meas in measurements[:100]:
                entries.append(
                    MetricEntry(
                        name=meas,
                        datasource_uid=datasource.uid,
                        datasource_name=datasource.name,
                        datasource_type=datasource.type,
                        query_language="flux" if is_flux else self.query_language,
                    )
                )

        logger.info("influxdb_metrics_discovered", datasource=datasource.name, count=len(entries))
        return entries
