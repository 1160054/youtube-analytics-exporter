# main.py
from __future__ import annotations

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import argparse
import shutil
from typing import List, Optional, Dict

# ===============================
# 設定
# ===============================
SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",  # Data API で動画タイトル/ID取得に必要
]

START_DATE_DEFAULT = (date.today() - timedelta(days=90)).isoformat()
END_DATE_DEFAULT = date.today().isoformat()

OUT_DIR = Path("yt_analytics_data")
OUT_DIR.mkdir(exist_ok=True)
DOWNLOAD_ZIP_PATH = Path("yt_analytics_data.zip")

TOKEN_PATH = Path("token.json")
CLIENT_SECRET = "client_secret.json"  # 必要に応じて変更

# デフォルトメトリクス（YouTube Analytics API v2 の有効名称）
DEFAULT_METRICS = (
    "views,estimatedMinutesWatched,averageViewDuration,"
    "likes,comments,shares,subscribersGained,subscribersLost"
)

# よくある別名→正規名称のマッピング
METRIC_ALIASES = {
    "watchTime": "estimatedMinutesWatched",
    "avgViewDuration": "averageViewDuration",
    "avg_view_duration": "averageViewDuration",
    "estimated_watch_time_minutes": "estimatedMinutesWatched",
}


# ===============================
# 認証
# ===============================
def get_credentials(force_reauth: bool = False) -> Credentials:
    if force_reauth and TOKEN_PATH.exists():
        TOKEN_PATH.unlink()

    creds: Optional[Credentials] = None
    if TOKEN_PATH.exists():
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        except Exception:
            creds = None

    def needs_reauth(c: Optional[Credentials]) -> bool:
        if not c:
            return True
        if not c.valid and not c.refresh_token:
            return True
        have = set(c.scopes or [])
        need = set(SCOPES)
        return not need.issubset(have)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception:
            creds = None

    if needs_reauth(creds):
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET, SCOPES)
        creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    return creds


def build_yt_analytics_service(creds: Credentials):
    return build("youtubeAnalytics", "v2", credentials=creds)


def build_yt_data_service(creds: Credentials):
    return build("youtube", "v3", credentials=creds)


# ===============================
# 出力ユーティリティ
# ===============================
def reset_out_dir():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for p in OUT_DIR.iterdir():
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


# ===============================
# Data API: 動画ID/タイトル取得
# ===============================
def parse_channel_id_from_ids(ids: str) -> Optional[str]:
    if not ids:
        return None
    parts = ids.split("==")
    if len(parts) == 2 and parts[0] == "channel":
        return None if parts[1] == "MINE" else parts[1]
    return None


def get_uploads_playlist_id(youtube, channel_id: Optional[str]) -> str:
    try:
        if channel_id:
            resp = youtube.channels().list(
                part="contentDetails",
                id=channel_id,
                maxResults=1,
            ).execute()
        else:
            resp = youtube.channels().list(
                part="contentDetails",
                mine=True,
                maxResults=1,
            ).execute()
    except HttpError as e:
        msg = str(e)
        if "accessNotConfigured" in msg or "has not been used" in msg:
            raise RuntimeError(
                "YouTube Data API v3 が未有効（または無効化）です。GCP の対象プロジェクトで有効化してください。"
            ) from e
        if "insufficientPermissions" in msg or "Insufficient Permission" in msg:
            raise RuntimeError(
                "認可スコープが不足しています。--reauth で再認証し、youtube.readonly を許可してください。"
            ) from e
        raise

    items = resp.get("items", [])
    if not items:
        raise RuntimeError("チャンネル情報が取得できませんでした（権限またはチャンネルIDを確認）。")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_video_ids_from_uploads(youtube, uploads_playlist_id: str) -> List[str]:
    video_ids: List[str] = []
    page_token = None
    while True:
        pl_resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=page_token,
        ).execute()
        for item in pl_resp.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])
        page_token = pl_resp.get("nextPageToken")
        if not page_token:
            break
    return video_ids


def get_all_video_ids(youtube, ids: str) -> List[str]:
    channel_id = parse_channel_id_from_ids(ids)
    uploads_playlist_id = get_uploads_playlist_id(youtube, channel_id)
    return get_video_ids_from_uploads(youtube, uploads_playlist_id)


def chunked(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def get_video_title_map(youtube, video_ids: List[str]) -> Dict[str, str]:
    """
    videos.list(part=snippet) を使って videoId -> title の辞書を返す。
    """
    title_map: Dict[str, str] = {}
    for chunk in chunked(video_ids, 50):  # APIの上限に合わせて分割
        resp = youtube.videos().list(
            part="snippet",
            id=",".join(chunk),
            maxResults=50
        ).execute()
        for item in resp.get("items", []):
            vid = item["id"]
            title = item.get("snippet", {}).get("title", "")
            title_map[vid] = title
    return title_map


# ===============================
# Analytics API ヘルパー
# ===============================
def run_report(yta, *, ids, startDate, endDate, metrics,
               dimensions=None, filters=None, sort=None, page_size=200) -> pd.DataFrame:
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


def _sanitize_sort_for_day_only(sort: Optional[str]) -> str:
    if not sort:
        return "day"
    parts = [p.strip() for p in sort.split(",") if p.strip()]
    parts = [p for p in parts if p.lstrip("-") != "video"]
    if not parts:
        parts = ["day"]
    if all(p.lstrip("-") != "day" for p in parts):
        parts.insert(0, "day")
    return ",".join(parts)


def sanitize_metrics(metrics: str) -> str:
    cleaned: List[str] = []
    for raw in metrics.split(","):
        m = raw.strip()
        if not m:
            continue
        m = METRIC_ALIASES.get(m, m)
        if m not in cleaned:
            cleaned.append(m)
    return ",".join(cleaned)


# ===============================
# 取得ロジック
# ===============================
def videos_daily_via_loop(yta, youtube, *, ids, startDate, endDate, metrics, sort=None) -> pd.DataFrame:
    video_ids = get_all_video_ids(youtube, ids)
    print(f"動画IDを {len(video_ids)} 件取得しました。日別×動画で集計します…")

    # タイトルを一括取得
    title_map = {}
    try:
        title_map = get_video_title_map(youtube, video_ids)
    except Exception as e:
        print(f"[WARN] タイトル取得に失敗しました（継続します）。詳細: {e}")

    safe_sort = _sanitize_sort_for_day_only(sort)
    dfs = []
    for i, vid in enumerate(video_ids, start=1):
        df = run_report(
            yta,
            ids=ids,
            startDate=startDate,
            endDate=endDate,
            metrics=metrics,
            dimensions="day",
            filters=f"video=={vid}",
            sort=safe_sort,
        )
        if not df.empty:
            df.insert(0, "videoId", vid)
            df.insert(1, "videoTitle", title_map.get(vid, ""))  # ← タイトル列
            dfs.append(df)
        if i % 50 == 0:
            print(f"  {i} / {len(video_ids)} 本処理…")

    if dfs:
        merged = pd.concat(dfs, ignore_index=True)
        cols = list(merged.columns)
        # videoId, videoTitle, day を先頭に整える
        for k in ["videoId", "videoTitle", "day"]:
            if k in cols:
                cols.remove(k)
        merged = merged[["videoId", "videoTitle", "day"] + cols]
        return merged
    else:
        return pd.DataFrame(columns=["videoId", "videoTitle", "day"] + [m.strip() for m in metrics.split(",")])


def videos_daily_via_analytics_only(yta, youtube_opt, *, ids, startDate, endDate, metrics, sort=None) -> pd.DataFrame:
    df = run_report(
        yta,
        ids=ids,
        startDate=startDate,
        endDate=endDate,
        metrics=metrics,
        dimensions="video,day",
        sort=sort or "day",
    )
    if df.empty:
        return df

    # 列名整形
    if "video" in df.columns:
        df = df.rename(columns={"video": "videoId"})
    # 可能なら Data API でタイトルを一括取得
    df.insert(1, "videoTitle", "")  # 仮置き
    if youtube_opt is not None:
        try:
            uniq_ids = sorted(set(df["videoId"].astype(str).tolist()))
            title_map = get_video_title_map(youtube_opt, uniq_ids)
            df["videoTitle"] = df["videoId"].map(lambda x: title_map.get(str(x), ""))
        except Exception as e:
            print(f"[WARN] タイトル取得に失敗しました（継続します）。詳細: {e}")

    # 列順を videoId, videoTitle, day 先頭へ
    cols = list(df.columns)
    for k in ["videoId", "videoTitle", "day"]:
        if k in cols:
            cols.remove(k)
    df = df[["videoId", "videoTitle", "day"] + cols]
    return df


# ===============================
# CLI / メイン
# ===============================
def parse_args():
    parser = argparse.ArgumentParser(description="YouTube Analytics: 動画×日別レポート出力（reports.json 不要、タイトル列付き）")
    parser.add_argument("--start", default=START_DATE_DEFAULT, help="開始日 YYYY-MM-DD（デフォルト：過去90日）")
    parser.add_argument("--end", default=END_DATE_DEFAULT, help="終了日 YYYY-MM-DD（デフォルト：今日）")
    parser.add_argument("--ids", default="channel==MINE", help='対象IDs（例："channel==MINE" または "channel==UCxxxx"）')
    parser.add_argument("--metrics", default=DEFAULT_METRICS, help="カンマ区切りのメトリクス（別名は自動マッピング）")
    parser.add_argument("--sort", default="day,video", help="ソートキー（例：day,video / -day など）")
    parser.add_argument("--reauth", action="store_true", help="強制再認証（token.json を作り直す）")
    parser.add_argument("--video-ids-file", help="1行1IDの外部動画IDリスト。指定時は Data API を使わずこのリストで処理（タイトル取得にはData APIが必要）")
    parser.add_argument("--no-fallback", action="store_true", help="フォールバック（analytics-only）を無効化する")
    return parser.parse_args()


def main():
    args = parse_args()

    start_date = args.start
    end_date = args.end
    ids = args.ids
    metrics = sanitize_metrics(args.metrics)
    sort = args.sort

    print(f"Fetching data from {start_date} to {end_date}...")

    reset_out_dir()

    creds = get_credentials(force_reauth=args.reauth)
    yta = build_yt_analytics_service(creds)

    # Data API（タイトル取得にも使用）を用意（失敗しても継続）
    youtube_opt = None
    try:
        youtube_opt = build_yt_data_service(creds)
    except Exception as e:
        print(f"[WARN] YouTube Data API 初期化に失敗しました（タイトル付与はスキップ）。詳細: {e}")

    # 外部動画IDリストの指定がある場合
    if args.video_ids_file:
        video_ids = [line.strip() for line in Path(args.video_ids_file).read_text(encoding="utf-8").splitlines() if line.strip()]
        print(f"外部リストから動画IDを {len(video_ids)} 件読み込みました。日別×動画で集計します…")

        # タイトルマップ（可能なら取得）
        title_map = {}
        if youtube_opt is not None:
            try:
                title_map = get_video_title_map(youtube_opt, video_ids)
            except Exception as e:
                print(f"[WARN] タイトル取得に失敗しました（継続します）。詳細: {e}")

        safe_sort = _sanitize_sort_for_day_only(sort)
        dfs = []
        for i, vid in enumerate(video_ids, start=1):
            df = run_report(
                yta,
                ids=ids,
                startDate=start_date,
                endDate=end_date,
                metrics=metrics,
                dimensions="day",
                filters=f"video=={vid}",
                sort=safe_sort,
            )
            if not df.empty:
                df.insert(0, "videoId", vid)
                df.insert(1, "videoTitle", title_map.get(vid, ""))  # タイトル
                dfs.append(df)
            if i % 50 == 0:
                print(f"  {i} / {len(video_ids)} 本処理…")

        merged = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(
            columns=["videoId", "videoTitle", "day"] + [m.strip() for m in metrics.split(",")]
        )
        if not merged.empty and "day" in merged.columns:
            cols = list(merged.columns)
            for k in ["videoId", "videoTitle", "day"]:
                if k in cols:
                    cols.remove(k)
            merged = merged[["videoId", "videoTitle", "day"] + cols]

        out_path = OUT_DIR / f'videos_daily_{start_date}_{end_date}.csv'
        merged.to_csv(out_path, index=False, encoding="utf-8")
        print(f"videos_daily -> {out_path.name}")
        zip_out_dir()
        return

    # 通常：Data APIで動画IDを取得してループ方式（タイトル付）
    try:
        if youtube_opt is None:
            # Data API が使えない場合はここで例外にしてフォールバックへ
            raise RuntimeError("YouTube Data API が利用できません。")
        df = videos_daily_via_loop(
            yta, youtube_opt,
            ids=ids,
            startDate=start_date,
            endDate=end_date,
            metrics=metrics,
            sort=sort
        )
    except Exception as e:
        if args.no_fallback:
            raise
        print(f"[WARN] 動画IDの取得またはループ集計に失敗しました。フォールバックして Analytics の複合ディメンションで一括取得します。\n詳細: {e}")
        df = videos_daily_via_analytics_only(
            yta,
            youtube_opt,  # タイトル付与のため可能なら渡す
            ids=ids,
            startDate=start_date,
            endDate=end_date,
            metrics=metrics,
            sort="day",
        )

    out_path = OUT_DIR / f'videos_daily_{start_date}_{end_date}.csv'
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"videos_daily -> {out_path.name}")
    zip_out_dir()


if __name__ == "__main__":
    main()
