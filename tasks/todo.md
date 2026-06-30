# Datum (Dagster + Parquet + DuckDB medallion platform)

## Plan
See `tasks/plans/dagster-migration.md` — the detailed technical plan.

## Todo

### Phase 0: Scaffold + smoke test
- [x] Create repo at ~/repos/datalake
- [x] pyproject.toml with dagster, dagster-duckdb, dagster-webserver deps
- [x] dagster.yaml (telemetry off, concurrency pool duckdb_state=1)
- [x] Skeleton package: src/datalake/{definitions,resources,lake,config}.py
- [x] defs/{bronze,silver,gold,serving}/__init__.py stubs
- [x] uv sync
- [ ] dg dev boots — verify UI + no telemetry + concurrency pool visible
- [ ] No-op @asset with DuckDBResource — verify lifecycle + read_parquet

### Phase 1: Resources, lake, IG library import
- [ ] Wire sys.path or pip install -e for ig_pipeline library
- [ ] Verify ApifyResource.token resolves from env
- [ ] Verify GeminiResource.api_key resolves from env
- [ ] lake.py path helpers — verify file creation at env paths

### Phase 2: Bronze + silver (posts chain)
- [ ] bronze_posts_raw asset (dynamic partition, wraps trigger_run+poll_run+download_dataset)
- [ ] bronze_posts_parquet asset (mapped partition, .jsonl → typed .parquet)
- [ ] Extract silver_dataset(dataset_id) from ig_pipeline.silver.deduplicate_all()
- [ ] silver_posts asset (mapped partition, @asset(pool="duckdb_state"))
- [ ] Silver idempotency test (re-materialize = no duplicates)

### Phase 3: Gold + serving (posts chain)
- [ ] gold_analyses asset (bulk, GoldConfig.post_ids, @asset(pool="duckdb_state"))
- [ ] dim_time asset (@asset(pool="duckdb_state"))
- [ ] dim_profile asset (post-derived, @asset(pool="duckdb_state"))
- [ ] analytics_views asset (terminal, @asset(pool="duckdb_state"))

### Phase 4: Profile-details chain
- [ ] bronze_profile_details_raw/parquet assets
- [ ] dim_profile_from_details asset (SCD2, @asset(pool="duckdb_state"))
- [ ] dim_profile dual-upstream reconciliation (details wins)

### Phase 5: Schedules & sensor
- [ ] Weekly silver→gold→serving schedule
- [ ] Sensor: poll list_runs() → auto-create bronze partitions

### Phase 6: Tests
- [ ] materialize() per asset with fake resources (exercises asset wiring)
- [ ] Contract + edge-case tests (1:1 from plan inventory)
- [ ] Parity integration test vs ig-pipeline scripts

### Phase 7: Cutover & deprecation
- [ ] Run Dagster + old scripts in parallel; confirm parity
- [ ] Deprecate ~/repos/ig-pipeline
- [ ] Add history summary doc to this repo
- [ ] Update AGENTS.md, README, CI
