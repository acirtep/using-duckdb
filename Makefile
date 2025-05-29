.PHONY: format-fix get-log readme plot refresh-data

format-fix:
	ruff format && ruff check --fix && isort .

search-repos:
	uv run using_duckdb/search_repositories.py

readme:
	echo '# Repositories using `duckdb`' > README.md && \
	cat exported_records.md >> README.md

git-log:
	echo '|Name|Topics|Stars|Open Issues|Forks|Created At|Updated At|' > git_log.txt && \
	git log --follow -p --pretty=format:"" -- README.md | grep '^+|\[' | sed 's/^+//' >> git_log.txt && \
	git diff -- README.md | grep '^+|\[' | sed 's/^+//' >> git_log.txt

plot:
	uv run using_duckdb/plot_results.py

refresh-data: search-repos readme git-log plot
