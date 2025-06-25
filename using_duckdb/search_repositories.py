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
        duckdb_conn.read_json(f"{api_url}&per_page=100&page={page}").insert_into("github_raw_data")


def export_to_md(duckdb_conn):
    selected_data = (
        duckdb_conn.table("github_raw_data")
        .select("unnest(items, recursive := true)")
        .select("""
            concat_ws(
                '<br>',
                concat('[', name, '](', concat('https://github.com/', full_name),')'),
                coalesce(description, ' '),
                concat('**License** ', coalesce(name_1, 'unknown')),
                concat('**Owner** ', login)
            ) as repo_details,
            coalesce(topics, '[]')::varchar as topics,
            coalesce(stargazers_count, 0) as stars,
            coalesce(open_issues_count, 0) as open_issues,
            coalesce(forks, 0) as forks,
            created_at,
            updated_at,
            login,
            coalesce(fork, false) as fork,
            stars + open_issues + forks as activity_count
        """)
        .filter("""
            login != 'duckdb'
            and not fork
            and activity_count >= 3
        """)
        .select("""
            NULL,
            repo_details,
            topics,
            stars,
            open_issues,
            forks,
            created_at,
            updated_at,
            NULL
        """)
        .order("""
            created_at desc,
            updated_at desc,
            stars desc
        """)
    )
    (
        duckdb_conn.sql("""
                select 
                    NULL,
                    'Name',
                    'Topics',
                    'Stars',
                    'Open Issues',
                    'Forks',
                    'Created At',
                    'Updated At',
                    NULL
            """)
        .union(
            duckdb_conn.sql("""
            select 
                NULL as '',
                '--' as "Name",
                '--' as "Topics",
                '--' as "Stars",
                '--' as "Open Issues",
                '--' as "Forks",
                '--' as "Created At",
                '--' as "Updated At",
                NULL as ''
        """)
        )
        .union(selected_data)
    ).to_csv("./exported_records.md", sep="|", header=False)


def export_to_readme(duckdb_conn):
    selected_data = (
        (
            duckdb_conn.table("github_raw_data")
            .select("unnest(items, recursive := true)")
            .select("""
            concat_ws(
                '<br>',
                concat('[', name, '](', concat('https://github.com/', full_name),')'),
                coalesce(description, ' '),
                concat('**License** ', coalesce(name_1, 'unknown')),
                concat('**Owner** ', login)
            ) as repo_details,
            coalesce(topics, '[]')::varchar as topics,
            coalesce(stargazers_count, 0) as stars,
            coalesce(open_issues_count, 0) as open_issues,
            coalesce(forks, 0) as forks,
            created_at,
            updated_at,
            login,
            coalesce(fork, false) as fork,
            stars + open_issues + forks as activity_count
        """)
            .filter("""
            login != 'duckdb'
            and not fork
            and activity_count >= 3
        """)
            .order("""
            created_at desc,
            updated_at desc,
            stars desc
        """)
        )
        .select("""
            concat(
                '|',
                concat_ws('|', repo_details, topics, stars, open_issues, forks, created_at, updated_at)
            ) as line
        """)
        .string_agg("line", sep="|\n")
        .fetchone()[0]
    )

    with open("README.md", "w") as readme_file:
        readme_file.write("# Repositories using `duckdb`\n")
        readme_file.write("|Name|Topics|Stars|Open Issues|Forks|Created At|Updated At|\n")
        readme_file.write(f"{duckdb_conn.sql("select concat(repeat('|--', 7),'|')").fetchone()[0]}\n")
        readme_file.write(f"{selected_data}|")


def main():
    duckdb_conn = get_duckdb_conn()
    get_search_requirements(duckdb_conn)
    export_to_md(duckdb_conn)


if __name__ == "__main__":
    main()
