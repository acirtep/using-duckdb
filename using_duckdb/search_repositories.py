import logging
from datetime import datetime
from datetime import timedelta
from math import ceil

from helpers import get_duckdb_conn

logging.basicConfig(
    level=logging.DEBUG,
    format="{asctime} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M:%S.%s",
)


logger = logging.getLogger(__file__)


def get_search_requirements(duckdb_conn):
    api_url = "https://api.github.com/search/repositories?q=duckdb"

    last_pushed_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    api_url = f"{api_url}+pushed:>={last_pushed_date}"

    (duckdb_conn.read_json(f"{api_url}&per_page=100&page=1")).to_table("github_raw_data")

    total_count_current_execution = (duckdb_conn.table("github_raw_data").select("total_count")).fetchone()[0]

    fetch_max_1000 = min(total_count_current_execution, 1000)

    logger.info(f"Number of records to fetch {fetch_max_1000}")

    number_pages = ceil(fetch_max_1000 / 100)

    for page in range(2, number_pages + 1):
        logger.info(f"Fetching {page} out of {number_pages}")
        (duckdb_conn.read_json(f"{api_url}&per_page=100&page={page}")).insert_into("github_raw_data")


def export_to_md(duckdb_conn):
    md_content = (
        duckdb_conn.table("github_raw_data")
        .select("unnest(items, recursive := true)")
        .select("""
                concat(
                    '|', 
                    concat_ws(
                        '|',
                        concat(
                            concat('[', name, '](', concat('https://github.com/', full_name),')'),
                            '<br>',
                            coalesce(description, ' '),
                            '<br>',
                            concat('**License** ', coalesce(name_1, 'unknown')),
                            '<br>',
                            concat('**Owner** ', login)
                        ),
                        coalesce(topics, '[]'),
                        coalesce(stargazers_count, 0),
                        coalesce(open_issues_count, 0),
                        coalesce(forks, 0),
                        created_at,
                        updated_at
                    ),
                    '|'
                ) as repo_line,
                stargazers_count,
                created_at,
                updated_at,
                login,
                coalesce(fork, false) as fork,
                coalesce(stargazers_count, 0) + coalesce(open_issues_count, 0) + coalesce(forks, 0) as activity_count
        """)
        .filter("""
            login != 'duckdb'
            and not fork
            and activity_count >= 3
        """)
        .order("created_at desc, updated_at desc, stargazers_count desc")
        .string_agg("repo_line", sep="\n")
    ).fetchone()[0]

    if not md_content:
        raise ValueError("No data returned")

    with open("./README.md", "w") as f_md:
        f_md.write("# Repositories using `duckdb` \n")
        f_md.write(f"A list of GitHub repositories, which have an update in the last 7 days, as of {datetime.now()}.")
        f_md.write("\n\n")
        f_md.write("|Name|Topics|Stars|Open Issues|Forks|Created At|Updated At|")
        f_md.write("\n|--|--|--|--|--|--|--|\n")
        f_md.write(md_content)


def main():
    duckdb_conn = get_duckdb_conn()
    get_search_requirements(duckdb_conn)
    export_to_md(duckdb_conn)


if __name__ == "__main__":
    main()
