import plotly.express as px
from helpers import get_duckdb_conn


def write_plot():
    duckdb_conn = get_duckdb_conn()
    px.scatter(
        duckdb_conn.read_csv(
            "./git_log.txt",
        )
        .select("""
            #2 as repo,
            #3 as topics,
            #4 as stars,
            #5 open_issues,
            #6 as forks,
            #7 as created_at,
            #8 as updated_at,
            if(stars + forks + open_issues = 0, 0, log(stars + forks + open_issues)) as log_activity_count,
            substr(repo, position('[' in repo) + 1 , position(']' in repo) - 2) as repo_name,
            count(distinct updated_at::date) over (partition by repo_name) as number_of_updates
        """)
        .order("updated_at"),
        x="updated_at",
        y="log_activity_count",
        color="repo_name",
        hover_name="repo",
        hover_data={
            "stars": True,
            "open_issues": True,
            "forks": True,
            "repo_name": False,
            "updated_at": True,
            "log_activity_count": False,
        },
        size="number_of_updates",
        text="repo_name",
        title="Repositories, using duckdb, updated in the last 7 days",
    ).update_layout(showlegend=False).update_yaxes(visible=False, showticklabels=False).write_html("./plot.html")


if __name__ == "__main__":
    write_plot()
