# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- `get_job_details` no longer fails outright when the Jobs API 400s with a
  misleading "does not exist" for a job that's actually just ACL-gapped
  (this connection's token lacks `CAN_VIEW`). It now falls back to
  `system.lakeflow.jobs`/`job_tasks` for partial info (name, creator/run_as,
  trigger type, task keys) and marks the result `partial` with an explanation,
  instead of erroring with no information. Genuinely missing job IDs still
  raise as before. (de-repo-artifact#90, option 3 of that issue's proposals)

## [0.2.0] - 2026-01-12

### Added
- **Multi-workspace support** — Configure multiple Databricks workspaces in `auth.yaml` and select per-call with `workspace` parameter
- **Configuration split** — Separate `auth.yaml` (secrets, gitignored) from `config.json` (tunable settings, committed)
- **CI/CD pipeline** — GitHub Actions with pytest, Black formatting, and Mypy type checking
- **CODEOWNERS** — Automatic review requests for PRs
- **PyArrow support** — Enable CloudFetch and improved performance for large datasets

### Changed
- Improved README with badges, collapsible sections, and clearer Cursor setup instructions
- Better error messages for workspace configuration issues

### Fixed
- Connection pool now validates individual connections rather than using global health cache
- Job manager correctly parses multi-task job structures from Databricks Jobs API 2.1
- All Mypy type errors resolved

## [0.1.0] - 2026-01-10

### Added
- Initial release
- **SQL Tools** — `execute_sql_query`, `discover_schemas`, `discover_tables`, `describe_table`, `get_table_sample`, `connection_health`
- **Jobs Tools** — `list_jobs`, `get_job_details`, `get_job_runs`, `trigger_job`, `cancel_job_run`, `get_job_run_output`
- **Observability** — `cache_stats`, `performance_stats`
- **Production reliability** — Connection pooling, retry logic with exponential backoff, circuit breakers
- **Bounded SQL output** — Configurable row/byte/cell limits to prevent OOM
- **Query caching** — Optional TTL-based caching for repeated queries

[Unreleased]: https://github.com/laraib-sidd/bricks-and-context/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/laraib-sidd/bricks-and-context/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/laraib-sidd/bricks-and-context/releases/tag/v0.1.0
