"""ops.sqlite application-database contract.

The dashboard writes this database; the pipeline reads it. It holds operational
identity and cache state only — never analytical data, which lives in DuckDB.

Modules are import-time inert: they declare functions and constants, and open the
database only when a caller passes a connection.
"""
