"""Pydantic contract models for the Graphite metrics/find API (via Grafana proxy).

Tacit reads:
  GET .../metrics/find?query=pattern -> [{id, text, leaf, expandable, allowChildren}]
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, RootModel


class GraphiteNode(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    text: str
    leaf: int = 1
    expandable: int = 0
    allowChildren: int = 0


class GraphiteFindResponse(RootModel[list[GraphiteNode]]):
    pass
