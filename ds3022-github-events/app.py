from shiny import App, render, ui
import duckdb
import pandas as pd
import plotly.express as px

# Connect to the DuckDB database
con = duckdb.connect("data/github.duckdb")

# Define the UI
app_ui = ui.page_fluid(
    ui.h1("GitHub Events Dashboard"),
    ui.navset_tab(
        ui.nav("Event Types Distribution", ui.output_plot("event_types_plot")),
        ui.nav("Top Repositories", ui.output_plot("top_repos_plot")),
        ui.nav("Events Over Time", ui.output_plot("events_over_time_plot")),
        ui.nav("Top Organizations", ui.output_plot("top_orgs_plot")),
        ui.nav("Ref Type Distribution", ui.output_plot("ref_type_plot")),
    ),
)

# Define the server logic
def server(input, output, session):
    # Event Types Distribution
    @output
    @render.plot
    def event_types_plot():
        df_event_types = con.execute("""
            SELECT type, COUNT(*) AS count
            FROM events
            WHERE type IS NOT NULL
            GROUP BY type
            ORDER BY count DESC
        """).fetchdf()
        fig = px.bar(df_event_types, x="type", y="count", title="Event Types Distribution")
        return fig

    # Events Over Time
    @output
    @render.plot
    def events_over_time_plot():
        df_events_over_time = con.execute("""
            SELECT DATE_TRUNC('hour', CAST(created_at AS TIMESTAMP)) AS hour, COUNT(*) AS events
            FROM events
            WHERE created_at IS NOT NULL
            GROUP BY hour
            ORDER BY hour
        """).fetchdf()
        fig = px.line(df_events_over_time, x="hour", y="events", title="Events Over Time")
        return fig

    # Top Organizations by Event Count
    @output
    @render.plot
    def top_orgs_plot():
        df_top_orgs = con.execute("""
            SELECT org, COUNT(*) AS count
            FROM events
            WHERE org IS NOT NULL
            GROUP BY org
            ORDER BY count DESC
            LIMIT 10
        """).fetchdf()
        fig = px.bar(df_top_orgs, x="org", y="count", title="Top Organizations by Event Count")
        return fig

    # Ref Type Distribution
    @output
    @render.plot
    def ref_type_plot():
        df_ref_type = con.execute("""
            SELECT ref_type, COUNT(*) AS count
            FROM events
            WHERE ref_type IS NOT NULL
            GROUP BY ref_type
            ORDER BY count DESC
        """).fetchdf()
        fig = px.pie(df_ref_type, names="ref_type", values="count", title="Ref Type Distribution")
        return fig

# Create the app
app = App(app_ui, server)