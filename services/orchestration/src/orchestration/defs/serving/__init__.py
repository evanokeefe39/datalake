"""Serving — the semantic layer the dashboard reads.

`dims.py` holds the durable dimensions, `metrics.py` the canonical metric
views (the ONE definition of a metric), `marts.py` the cross-domain analytic
marts, `views.py` the consumer views, and `checks.py` the serving checks.

Each module exports an `ASSETS` list; `definitions.py` composes them.
"""
