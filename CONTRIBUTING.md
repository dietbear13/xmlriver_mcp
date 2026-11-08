# Contributing

Use Python 3.11+, `uv sync --locked`, then `uv run pytest`, `uv run ruff check .`,
`uv run mypy`, `uv build`. Mock paid endpoints in tests. Never commit real keys,
customer data or request URLs containing authentication. See SECURITY.md.
