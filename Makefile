.PHONY: format-fix

format-fix:
	ruff format && ruff check --fix && isort .
