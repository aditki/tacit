"""Adapter for AWS CloudWatch datasource.

CloudWatch organises metrics as Namespace → MetricName → Dimensions.
Grafana exposes these via its datasource resource API:
  POST /api/datasources/uid/{uid}/resources/namespaces
  POST /api/datasources/uid/{uid}/resources/metrics   {namespace, region}
  POST /api/datasources/uid/{uid}/resources/dimension-keys  {namespace, region, metricName}
"""

from __future__ import annotations

import structlog

from dashforge.grafana.adapters.base import DatasourceAdapter
from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DatasourceInfo, MetricEntry

logger = structlog.get_logger()

# Namespaces most relevant during incidents — searched first.
PRIORITY_NAMESPACES = [
    "AWS/ELB",
    "AWS/ApplicationELB",
    "AWS/NetworkELB",
    "AWS/EC2",
    "AWS/ECS",
    "AWS/RDS",
    "AWS/Lambda",
    "AWS/SQS",
    "AWS/SNS",
    "AWS/DynamoDB",
    "AWS/ElastiCache",
    "AWS/ApiGateway",
    "AWS/S3",
    "AWS/Kinesis",
    "AWS/CloudFront",
]

# Maps common observability keywords to relevant CloudWatch namespaces.
KEYWORD_NAMESPACE_MAP: dict[str, list[str]] = {
    "elb": ["AWS/ELB", "AWS/ApplicationELB", "AWS/NetworkELB"],
    "alb": ["AWS/ApplicationELB"],
    "nlb": ["AWS/NetworkELB"],
    "load_balancer": ["AWS/ELB", "AWS/ApplicationELB", "AWS/NetworkELB"],
    "loadbalancer": ["AWS/ELB", "AWS/ApplicationELB", "AWS/NetworkELB"],
    "ec2": ["AWS/EC2"],
    "instance": ["AWS/EC2"],
    "rds": ["AWS/RDS"],
    "database": ["AWS/RDS", "AWS/DynamoDB", "AWS/ElastiCache"],
    "db": ["AWS/RDS", "AWS/DynamoDB"],
    "lambda": ["AWS/Lambda"],
    "function": ["AWS/Lambda"],
    "sqs": ["AWS/SQS"],
    "queue": ["AWS/SQS"],
    "ecs": ["AWS/ECS"],
    "container": ["AWS/ECS"],
    "api": ["AWS/ApiGateway"],
    "gateway": ["AWS/ApiGateway"],
    "cache": ["AWS/ElastiCache"],
    "redis": ["AWS/ElastiCache"],
    "s3": ["AWS/S3"],
    "storage": ["AWS/S3"],
    "kinesis": ["AWS/Kinesis"],
    "stream": ["AWS/Kinesis"],
    "cdn": ["AWS/CloudFront"],
    "cloudfront": ["AWS/CloudFront"],
    "5xx": ["AWS/ApplicationELB", "AWS/ELB", "AWS/ApiGateway"],
    "500": ["AWS/ApplicationELB", "AWS/ELB", "AWS/ApiGateway"],
    "4xx": ["AWS/ApplicationELB", "AWS/ELB", "AWS/ApiGateway"],
    "latency": ["AWS/ApplicationELB", "AWS/ELB", "AWS/ApiGateway", "AWS/Lambda", "AWS/DynamoDB"],
    "error": ["AWS/ApplicationELB", "AWS/ELB", "AWS/Lambda", "AWS/SQS", "AWS/ApiGateway"],
    "cpu": ["AWS/EC2", "AWS/ECS", "AWS/RDS", "AWS/ElastiCache"],
    "memory": ["AWS/EC2", "AWS/ECS", "AWS/ElastiCache"],
    "disk": ["AWS/EC2", "AWS/RDS"],
    "network": ["AWS/EC2", "AWS/ELB", "AWS/ApplicationELB"],
    "throttle": ["AWS/DynamoDB", "AWS/Lambda", "AWS/ApiGateway", "AWS/Kinesis"],
}


def _select_namespaces(keywords: list[str], available: list[str]) -> list[str]:
    """Pick CloudWatch namespaces relevant to the intent keywords.

    Strategy (narrow → broad):
    1. Match keywords against KEYWORD_NAMESPACE_MAP → intersect with available.
    2. If nothing matched, use only the top 5 priority namespaces (not 8+).
    3. If still empty, return whatever priority namespaces exist.
    """
    matched: set[str] = set()
    kw_lower = [k.lower() for k in keywords]

    for kw in kw_lower:
        for pattern, namespaces in KEYWORD_NAMESPACE_MAP.items():
            if pattern in kw or kw in pattern:
                matched.update(namespaces)

    available_set = set(available)

    if matched:
        result = [ns for ns in matched if ns in available_set]
        if result:
            return result[:10]

    # Narrow fallback: only top-5 priority namespaces to limit noise
    result = [ns for ns in PRIORITY_NAMESPACES[:5] if ns in available_set]
    if result:
        logger.info("cloudwatch_namespace_fallback", reason="no_keyword_match", namespaces=result)
        return result

    # Last resort: any priority namespace that exists
    result = [ns for ns in PRIORITY_NAMESPACES if ns in available_set]
    return result[:10]


class CloudWatchAdapter(DatasourceAdapter):

    @property
    def query_language(self) -> str:
        return "cloudwatch"

    @property
    def supported_types(self) -> set[str]:
        return {"cloudwatch"}

    async def discover_metrics(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        keywords: list[str],
    ) -> list[MetricEntry]:
        default_region = datasource.json_data.get("defaultRegion", "us-east-1")
        entries: list[MetricEntry] = []

        # 1. List available namespaces
        try:
            ns_resp = await client.datasource_resource(datasource.uid, "namespaces", {"region": default_region})
            if isinstance(ns_resp, list):
                available_ns: list[str] = ns_resp
            elif isinstance(ns_resp, dict):
                available_ns = list(ns_resp.keys())
            else:
                available_ns = []
        except Exception:
            logger.warning(
                "cloudwatch_namespace_list_hard_failure",
                datasource=datasource.name,
                fallback="static_priority_namespaces",
            )
            available_ns = PRIORITY_NAMESPACES

        # 2. Select namespaces relevant to the intent
        target_ns = _select_namespaces(keywords, available_ns)
        logger.info("cloudwatch_namespaces_selected", datasource=datasource.name, namespaces=target_ns)

        # 3. For each namespace, list metrics
        for ns in target_ns:
            try:
                metrics_resp = await client.datasource_resource(
                    datasource.uid,
                    "metrics",
                    {"region": default_region, "namespace": ns},
                )
                if isinstance(metrics_resp, list):
                    metric_names: list[str] = metrics_resp
                elif isinstance(metrics_resp, dict):
                    metric_names = list(metrics_resp.keys())
                else:
                    metric_names = []
            except Exception:
                logger.warning("cloudwatch_metrics_failed", datasource=datasource.name, namespace=ns)
                continue

            # 4. Keyword-filter metric names
            kw_lower = [k.lower() for k in keywords]
            for mname in metric_names:
                if any(k in mname.lower() for k in kw_lower) or not kw_lower:
                    entries.append(
                        MetricEntry(
                            name=f"{ns}/{mname}",
                            datasource_uid=datasource.uid,
                            datasource_name=datasource.name,
                            datasource_type=datasource.type,
                            query_language=self.query_language,
                            namespace=ns,
                        )
                    )

            # Also include all metrics in this namespace if few matched
            if not any(e.namespace == ns for e in entries):
                for mname in metric_names[:20]:
                    entries.append(
                        MetricEntry(
                            name=f"{ns}/{mname}",
                            datasource_uid=datasource.uid,
                            datasource_name=datasource.name,
                            datasource_type=datasource.type,
                            query_language=self.query_language,
                            namespace=ns,
                        )
                    )

        logger.info("cloudwatch_metrics_discovered", datasource=datasource.name, count=len(entries))
        return entries
