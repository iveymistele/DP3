#!/usr/bin/env python3
from prefect import flow, task
import requests, time, pandas as pd, duckdb
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

import os

# -------------------------
# Configuration
# -------------------------

REPOS = [
    "torvalds/linux",
    "microsoft/vscode",
    "facebook/react",
    "pytorch/pytorch",
    "kubernetes/kubernetes",
    "vercel/next.js",
    "flutter/flutter",
    "apache/spark",
    "ansible/ansible",
    "godotengine/godot"
]

BASE_URL = "https://api.github.com/repos/{repo}/events"

# Local + S3 paths
LOCAL_DB = "local.duckdb"
S3_BUCKET = "mistelehendershot-dp3"
S3_PREFIX = "duckdb_export"
AWS_REGION = "us-east-1"

# GitHub PAT
GITHUB_PAT = os.getenv("GITHUB_PAT")
if not GITHUB_PAT:
    raise ValueError("Missing GITHUB_PAT environment variable!")

# AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

if not AWS_ACCESS_KEY or not AWS_SECRET:
    raise ValueError("Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY in environment!")

headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GITHUB_PAT}"
}

RECORD_LIMIT = 300

# -------------------------
# Tasks
# -------------------------

@task(retries=3, retry_delay_seconds=15)
def fetch_repo_events(repo_name: str, max_pages: int = 5):
    """Fetch GitHub events with rate-limit handling."""
    all_events = []

    for page in range(1, max_pages + 1):
        url = f"{BASE_URL.format(repo=repo_name)}?per_page=100&page={page}"
        r = requests.get(url, headers=headers)

        if r.status_code == 403:
            reset_time = r.headers.get("X-RateLimit-Reset")
            if reset_time:
                wait_sec = max(
                    (datetime.fromtimestamp(int(reset_time), tz=timezone.utc) -
                     datetime.now(timezone.utc)).total_seconds(), 60
                )
                print(f"Rate limit hit for {repo_name}. Sleeping {int(wait_sec)}s.")
                time.sleep(wait_sec)
                return fetch_repo_events.fn(repo_name, max_pages=max_pages)
            else:
                print("403 no reset header — sleeping 5m.")
                time.sleep(300)
                return fetch_repo_events.fn(repo_name, max_pages=max_pages)

        elif r.status_code != 200:
            print(f"Failed {repo_name} page {page}: {r.status_code}")
            break

        events = r.json()
        if not events:
            break

        all_events.extend(events)

        poll_interval = int(r.headers.get("X-Poll-Interval", "60"))
        time.sleep(poll_interval)

    print(f"{repo_name}: {len(all_events)} events.")
    return all_events


@task
def flatten_events(events, repo_name):
    rows = []
    for e in events:
        repo_full = e.get("repo", {}).get("name", repo_name)
        parsed_org = repo_full.split("/")[0] if "/" in repo_full else None
        
        rows.append({
            "id": e.get("id"),
            "type": e.get("type"),
            "repo": repo_full,
            "actor": e.get("actor", {}).get("login"),
            "org": parsed_org,
            "created_at": e.get("created_at"),
            "action": e.get("payload", {}).get("action"),
            "ref": e.get("payload", {}).get("ref"),
            "ref_type": e.get("payload", {}).get("ref_type"),
            "inserted_at": datetime.utcnow(),
        })

    return pd.DataFrame(rows)



@task
def append_and_export(df):
    """Append events to local DuckDB and export to S3."""
    # Local DB
    conn = duckdb.connect(LOCAL_DB)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id VARCHAR,
            type VARCHAR,
            repo VARCHAR,
            actor VARCHAR,
            org VARCHAR,
            created_at TIMESTAMP,
            action VARCHAR,
            ref VARCHAR,
            ref_type VARCHAR,
            inserted_at TIMESTAMP
        )
    """)

    conn.register("df_view", df)
    conn.execute("INSERT INTO events SELECT * FROM df_view")

    # Configure S3
    conn.execute(f"SET s3_region='{AWS_REGION}';")
    conn.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{AWS_SECRET}';")

    # Export to your bucket
    s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}"
    print(f"Exporting DuckDB to {s3_path} ...")

    conn.execute(f"""
        EXPORT DATABASE '{s3_path}'
        (FORMAT PARQUET);
    """)

    conn.close()
    print("Exported to S3.")


# -------------------------
# Flow
# -------------------------

@flow(name="github-events-local-s3")
def github_events_flow():
    total_records = 0

    for repo in REPOS:
        events = fetch_repo_events(repo)
        df = flatten_events(events, repo)
        count = len(df)

        append_and_export(df)

        total_records += count
        print(f"{repo}: {count} rows added (total {total_records})")

        if total_records >= RECORD_LIMIT:
            print("Limit reached.")
            break

        time.sleep(1)

    print("Flow finished.")


# -------------------------
# Serve locally
# -------------------------
if __name__ == "__main__":
    github_events_flow.serve(
        name="local-github-s3-service",
        schedule=None
    )
