poetry run mypy --show-error-codes --install-types .
poetry run ruff format .
poetry run ruff check .
