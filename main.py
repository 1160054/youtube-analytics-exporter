from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from pathlib import Path
import pandas as pd
from datetime import date, timedelta
import shutil
import json
import argparse

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]
START_DATE = (date.today() - timedelta(days=90)).isoformat()
END_DATE = date.today().isoformat()

OUT_DIR = Path("yt_analytics_data")
OUT_DIR.mkdir(exist_ok=True)
DOWNLOAD_ZIP_PATH = Path("yt_analytics_data.zip")

TOKEN_PATH = Path("token.json")
CLIENT_SECRET = "client_secret.json"
REPORTS_PATH = Path("reports.json")


def get_credentials():
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET, SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
    return creds


def build_service():
    creds = get_credentials()
    return build("youtubeAnalytics", "v2", credentials=creds)


def load_reports(path: Path = REPORTS_PATH):
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def run_report(yta, *, ids, startDate, endDate, metrics,
               dimensions=None, filters=None, sort=None, page_size=200):
    rows_all = []
    start_index = 1
    headers = None
    while True:
        req = dict(
            ids=ids, startDate=startDate, endDate=endDate, metrics=metrics,
            maxResults=page_size, startIndex=start_index
        )
        if dimensions: req["dimensions"] = dimensions
        if filters:    req["filters"] = filters
        if sort:       req["sort"] = sort

        resp = yta.reports().query(**req).execute()
        if headers is None:
            headers = [h["name"] for h in resp.get("columnHeaders", [])]
        rows = resp.get("rows", []) or []
        if not rows:
            break
        rows_all.extend(rows)
        if len(rows) < page_size:
            break
        start_index += page_size

    return pd.DataFrame(rows_all, columns=headers or [])


def reset_out_dir():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for p in OUT_DIR.iterdir():
        # .gitkeep は残す
        if p.name == ".gitkeep":
            continue
        if p.is_file() or p.is_symlink():
            p.unlink()
        else:
            shutil.rmtree(p)


def zip_out_dir():
    target = DOWNLOAD_ZIP_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    base_name = str(target.with_suffix(""))
    shutil.make_archive(
        base_name=base_name,
        format="zip",
        root_dir=str(OUT_DIR.parent),
        base_dir=str(OUT_DIR.name),
    )
    print(f"Zipped to: {target}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=START_DATE)
    parser.add_argument("--end", default=END_DATE)
    parser.add_argument("--ids", default="channel==MINE")
    parser.add_argument("--reports", default=str(REPORTS_PATH))
    args = parser.parse_args()

    start_date = args.start
    end_date = args.end
    ids = args.ids
    reports_path = Path(args.reports)

    reset_out_dir()
    service = build_service()
    reports = load_reports(reports_path)

    print(f"Fetching data from {start_date} to {end_date}...")
    for cfg in reports:
        df = run_report(
            service,
            ids=ids,
            startDate=start_date,
            endDate=end_date,
            metrics=cfg["metrics"],
            dimensions=cfg.get("dimensions"),
            filters=cfg.get("filters"),
            sort=cfg.get("sort"),
            page_size=200,
        )
        out_path = OUT_DIR / f'{cfg["name"]}_{start_date}_{end_date}.csv'
        df.to_csv(out_path, index=False, encoding="utf-8")
        print(f"{cfg['name']} | {cfg.get('note', '')}")

    zip_out_dir()


if __name__ == "__main__":
    main()
