"""Cross-domain enrichment machinery — never duplicated per domain.

The seam (`provider`), the service-backed adapter, partitions, submit,
harvest, the landing writer, the media cache and the shared silver runtime
all live here and are parameterized by a domain's payload.
"""
