from shiny import App, render, ui, reactive
import duckdb
import plotly.express as px
from shinywidgets import output_widget, render_widget
from prefect_aws import AwsCredentials
from dotenv import load_dotenv
load_dotenv()
import os

# -----------------------
# S3 CONFIG
# -----------------------
AWS_REGION = "us-east-1"
S3_BUCKET = "mistelehendershot-dp3"
S3_PREFIX = "duckdb_export"    # exported by Prefect EXPORT DATABASE

# Path to all parquet files
S3_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/*.parquet"

# AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")


# -----------------------
# DuckDB connection factory
# -----------------------
def get_conn():

    conn = duckdb.connect()
    # configure S3 access
    conn.execute(f"SET s3_region='{AWS_REGION}';")
    conn.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{AWS_SECRET}';")
    conn.execute("SET s3_use_ssl=true;")
    conn.execute("SET s3_url_style='path';")
    return conn

# -----------------------
# Shiny UI
# -----------------------
app_ui = ui.page_fluid(
    ui.h1("GitHub Events Dashboard (S3-backed)"),
    ui.navset_tab(
        ui.nav_panel("Event Types Distribution", output_widget("event_types_plot")),
        ui.nav_panel("Top Organizations", output_widget("top_orgs_plot")),
        ui.nav_panel("Top Repositories", output_widget("top_repos_plot")),
        ui.nav_panel("Events Over Time", output_widget("events_over_time_plot")),
        ui.nav_panel("Ref Type Distribution", output_widget("ref_type_plot"))
    ),
)

# -----------------------
# Shiny Server
# -----------------------
def server(input, output, session):

    @reactive.poll(lambda: 30)
    def poll_s3():
        conn = get_conn()
        return conn.execute(
            f"SELECT MAX(created_at) FROM read_parquet('{S3_PATH}')"
        ).fetchone()[0]

    def load_query(query: str):
        conn = get_conn()
        return conn.execute(query).fetchdf()

    @output
    @render_widget
    def event_types_plot():
        _ = poll_s3()
        df = load_query(f"""
            SELECT type, COUNT(*) AS count
            FROM read_parquet('{S3_PATH}')
            WHERE type IS NOT NULL
            GROUP BY type
            ORDER BY count DESC
        """)
        return px.bar(df, x="type", y="count",  labels={"event_type": "Event Type", "event_count": "Count"},title="Event Types Distribution")

    @output
    @render_widget
    def top_orgs_plot():
        _ = poll_s3()
        df = load_query(f"""
            SELECT org, COUNT(*) AS count
            FROM read_parquet('{S3_PATH}')
            WHERE org IS NOT NULL
            GROUP BY org
            ORDER BY count DESC
            LIMIT 10
        """)
        return px.bar(df, x="org", y="count", title="Top Organizations")

    @output
    @render_widget
    def ref_type_plot():
        _ = poll_s3()
        df = load_query(f"""
            SELECT ref_type, COUNT(*) AS count
            FROM read_parquet('{S3_PATH}')
            WHERE ref_type IS NOT NULL
            GROUP BY ref_type
            ORDER BY count DESC
        """)
        return px.pie(df, names="ref_type", values="count", title="Ref Type Distribution")

    @output
    @render_widget
    def top_repos_plot():
        _ = poll_s3()
        df = load_query(f"""
            SELECT repo, COUNT(*) AS count
            FROM read_parquet('{S3_PATH}')
            GROUP BY repo
            ORDER BY count DESC
            LIMIT 10
        """)
        return px.bar(df, x="repo", y="count", title="Top Repositories")
    
    @output
    @render_widget
    def events_over_time_plot():
        _ = poll_s3()

        df = load_query(f"""
            SELECT
                date_trunc('minute', created_at) AS minute,
                COUNT(*) AS count
            FROM read_parquet('{S3_PATH}')
            WHERE created_at IS NOT NULL
            GROUP BY minute
            ORDER BY minute
        """)

    # Plotly line chart
        fig = px.line(
            df,
            x="minute",
            y="count",
            title="Events Over Time (Minute Resolution)",
            markers=True
        )

        fig.update_layout(
            xaxis_title="Time (minute)",
         yaxis_title="Event Count",
         hovermode="x unified"
        )

        return fig


app = App(app_ui, server)
