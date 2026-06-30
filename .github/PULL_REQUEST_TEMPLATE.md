---
name: Pull Request
about: Standard PR
title: ""
labels: ""
assignees: ""
---

## Summary

<!-- What does this change and why? -->

## Checklist

- [ ] Tests pass locally (`uv run pytest tests/ -v`)
- [ ] Ruff clean (`uv run ruff check src/ tests/`)
- [ ] Dagster definitions validate (`uv run dagster definitions validate`)
- [ ] Evidence attached (screenshot, log, test output)
