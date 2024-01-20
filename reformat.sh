poetry run mypy --show-error-codes --install-types .
poetry run ruff format --check .
poetry run ruff check .
