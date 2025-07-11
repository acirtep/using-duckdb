import plotly.express as px
from helpers import get_duckdb_conn


def write_plot():
    duckdb_conn = get_duckdb_conn()
    px.scatter(
        duckdb_conn.read_csv(
            "./git_log.md",
            delimiter="|",
            strict_mode=False,
            ignore_errors=True,
            null_padding=True,
            normalize_names=True,
            skiprows=2,
        )
        .select("""
        #2 as repo,
        #3 as topics,
        #4 as stars,
        #5 as open_issues,
        #6 as forks,
        strftime(#7, '%B %d, %Y, %H:%m' ) as created_at,
        #8 as updated_at,
        if(stars + forks + open_issues = 0, 0, log(stars + forks + open_issues)) as log_activity_count,
        substr(repo, position('[' in repo) + 1 , position(']' in repo) - 2) as repo_name,
        count(distinct updated_at::date) over (partition by repo_name) as number_of_updates,
        row_number() over (partition by repo_name order by updated_at desc) as rn
    """)
        .filter("rn = 1")
        .order("updated_at"),
        x="updated_at",
        y="log_activity_count",
        labels={
            "log_activity_count": "Activity count, based on stars, open issues and forks",
            "updated_at": "Updated Date",
        },
        color="repo_name",
        hover_name="repo",
        hover_data={
            "stars": True,
            "open_issues": True,
            "forks": True,
            "repo_name": False,
            "created_at": True,
            "updated_at": True,
            "log_activity_count": False,
        },
        size="number_of_updates",
        text="repo_name",
        title="Repositories mentioning duckdb",
    ).update_layout(showlegend=False).update_yaxes(showticklabels=False).write_html("./plot.html")


if __name__ == "__main__":
    write_plot()
