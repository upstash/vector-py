python3 -m poetry run mypy --show-error-codes --install-types .
python3 -m poetry run ruff format .
python3 -m poetry run ruff check .
