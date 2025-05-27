.PHONY: format-fix, get-log

format-fix:
	ruff format && ruff check --fix && isort .


git-log:
	echo '|Name|Topics|Stars|Open Issues|Forks|Created At|Updated At|' > git_log.txt && \
	git log --follow -p --pretty=format:"" -- README.md | grep '^+|\[' | sed 's/^+//' >> git_log.txt && \
	git diff -- README.md | grep '^+|\[' | sed 's/^+//' >> git_log.txt
