import os

import duckdb


def get_duckdb_conn():
    conn = duckdb.connect()
    conn.sql(f"""
        CREATE SECRET http_auth (
            TYPE http,
            BEARER_TOKEN '{os.getenv("READ_PUBLIC_REPO_TOKEN")}',
            SCOPE 'https://api.github.com/search'
        );
    """)
    return conn
