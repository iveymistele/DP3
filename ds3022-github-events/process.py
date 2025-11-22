import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the DuckDB database
con = duckdb.connect("data/github.duckdb")



# 1. Event Types Distribution
df_event_types = con.execute("""
    SELECT type, COUNT(*) AS count
    FROM events
    WHERE type IS NOT NULL
    GROUP BY type
    ORDER BY count DESC
""").fetchdf()

# Plot: Event Types Distribution
df_event_types.plot(x='type', y='count', kind='bar', figsize=(10, 5), title='Event Types Distribution', legend=False)
plt.xlabel('Event Type')
plt.ylabel('Count')
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y')
plt.show()

# 2. Top Repositories by Event Count
df_top_repos = con.execute("""
    SELECT repo, COUNT(*) AS count
    FROM events
    WHERE repo IS NOT NULL
    GROUP BY repo
    ORDER BY count DESC
    LIMIT 10
""").fetchdf()

# Plot: Top Repositories by Event Count
df_top_repos.plot(x='repo', y='count', kind='bar', figsize=(10, 5), title='Top Repositories by Event Count', legend=False)
plt.xlabel('Repository')
plt.ylabel('Count')
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y')
plt.show()