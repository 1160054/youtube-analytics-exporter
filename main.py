from __future__ import annotations

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

from pathlib import Path
from datetime import date, timedelta, datetime, timezone
import pandas as pd
import argparse
import shutil
from typing import List, Optional, Dict
import csv
import re

START_DATE_DEFAULT = (date.today() - timedelta(days=90)).isoformat()
END_DATE_DEFAULT = date.today().isoformat()

OUT_DIR = Path("yt_analytics_data")
OUT_DIR.mkdir(exist_ok=True)
DOWNLOAD_ZIP_PATH = Path("yt_analytics_data.zip")

TOKEN_PATH = Path("token.json")
CLIENT_SECRET = "client_secret.json"

DEFAULT_METRICS = (
    "views,estimatedMinutesWatched,averageViewDuration,"
    "likes,comments,shares,subscribersGained,subscribersLost"
)

METRIC_ALIASES = {
    "watchTime": "estimatedMinutesWatched",
    "avgViewDuration": "averageViewDuration",
    "avg_view_duration": "averageViewDuration",
    "estimated_watch_time_minutes": "estimatedMinutesWatched",
}

SCOPES_BASE = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]

SCOPE_FORCE_SSL = "https://www.googleapis.com/auth/youtube.force-ssl"

RE_CTRL = re.compile(r"[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]")


def clean_text(s: Optional[str]) -> str:
    """
    CSVで崩れやすい箇所を軽く正規化
      - 制御文字の除去
      - CRLF/CR を LF に正規化（Excel出力時は最終的に CRLF で書き出し）
    """
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = RE_CTRL.sub("", s)
    return s


def get_credentials(required_scopes: List[str],
                    force_reauth: bool = False) -> Credentials:
    if force_reauth and TOKEN_PATH.exists():
        TOKEN_PATH.unlink()

    creds: Optional[Credentials] = None
    if TOKEN_PATH.exists():
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH,
                                                          required_scopes)
        except Exception:
            creds = None

    def has_all_scopes(c: Optional[Credentials]) -> bool:
        if not c:
            return False
        have = set(c.scopes or [])
        need = set(required_scopes)
        return need.issubset(have)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception:
            creds = None

    if not has_all_scopes(creds):
        print("[INFO] 必要スコープが不足しています。ブラウザで再認証を行います。")
        print("      必要スコープ:", ", ".join(required_scopes))
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET,
                                                         required_scopes)
        creds = flow.run_local_server(port=0, prompt="consent")
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    print("[INFO] 付与されたスコープ:",
          ", ".join(sorted(set(creds.scopes or []))))
    if not has_all_scopes(creds):
        raise RuntimeError(
            "必要スコープが付与されていません。認証画面で全ての許可を与えたかご確認ください。")
    return creds


def build_yt_analytics_service(creds: Credentials):
    return build("youtubeAnalytics", "v2", credentials=creds)


def build_yt_data_service(creds: Credentials):
    return build("youtube", "v3", credentials=creds)


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
                "YouTube Data API v3 が未有効です。GCP コンソールで有効化してください。") from e
        if "insufficientPermissions" in msg or "Insufficient Permission" in msg:
            raise RuntimeError(
                "スコープ不足です。--reauth で再認証してください。") from e
        raise

    items = resp.get("items", [])
    if not items:
        raise RuntimeError(
            "チャンネル情報が取得できませんでした。権限/チャンネルIDをご確認ください。")
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
    title_map: Dict[str, str] = {}
    for chunk in chunked(video_ids, 50):
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


def iso_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(
        timezone.utc)


def fetch_latest_comments(
        youtube,
        video_ids: List[str],
        title_map: Dict[str, str],
        per_video: int,
        since_iso: Optional[str],
        until_iso: Optional[str],
        include_author_channel: bool = True,
) -> pd.DataFrame:
    """
    各動画の最新トップレベルコメントを per_video 件ずつ取得（UTC基準で期間フィルタ）。
    コメント無効や権限不足等はスキップ。
    """
    rows = []
    since_dt = iso_to_dt(since_iso + "T00:00:00Z") if since_iso else None
    until_dt = iso_to_dt(until_iso + "T23:59:59Z") if until_iso else None

    SKIP_MARKERS = (
        "insufficientPermissions",
        "forbidden",
        "commentsDisabled",
        "disabled comments",
        "videoNotFound",
    )

    for idx, vid in enumerate(video_ids, start=1):
        remaining = per_video
        page_token = None
        while remaining > 0:
            page_size = min(remaining, 100)
            try:
                resp = youtube.commentThreads().list(
                    part="snippet",
                    videoId=vid,
                    maxResults=page_size,
                    order="time",
                    textFormat="plainText",
                    pageToken=page_token,
                ).execute()
            except HttpError as e:
                msg = str(e)
                if any(marker in msg for marker in SKIP_MARKERS):
                    print(f"[WARN] コメント取得スキップ: videoId={vid} | {e}")
                    break
                raise

            items = resp.get("items", [])
            if not items:
                break

            for item in items:
                s = item["snippet"]
                top = s["topLevelComment"]["snippet"]
                published = iso_to_dt(top["publishedAt"])
                if since_dt and published < since_dt:
                    continue
                if until_dt and published > until_dt:
                    continue
                author_channel_id = ""
                if include_author_channel:
                    author_channel = top.get("authorChannelId", {})
                    author_channel_id = author_channel.get("value",
                                                           "") if isinstance(
                        author_channel, dict) else ""
                rows.append({
                    "videoId": vid,
                    "videoTitle": title_map.get(vid, ""),
                    "commentId": s["topLevelComment"]["id"],
                    "authorDisplayName": clean_text(
                        top.get("authorDisplayName", "")),
                    "authorChannelId": author_channel_id,
                    "text": clean_text(top.get("textDisplay", "")),
                    "likeCount": top.get("likeCount", 0),
                    "publishedAt": top.get("publishedAt", ""),
                    "updatedAt": top.get("updatedAt", ""),
                    "totalReplyCount": s.get("totalReplyCount", 0),
                })

            remaining -= len(items)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        if idx % 20 == 0:
            print(f"  コメント取得: {idx} / {len(video_ids)} 本処理…")

    return pd.DataFrame(rows)


def run_report(yta, *, ids, startDate, endDate, metrics,
               dimensions=None, filters=None, sort=None,
               page_size=200) -> pd.DataFrame:
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


def videos_daily_via_loop(yta, youtube, *, ids, startDate, endDate, metrics,
                          sort=None) -> pd.DataFrame:
    video_ids = get_all_video_ids(youtube, ids)
    print(f"動画IDを {len(video_ids)} 件取得しました。日別×動画で集計します…")

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
            df.insert(1, "videoTitle", title_map.get(vid, ""))
            dfs.append(df)
        if i % 50 == 0:
            print(f"  {i} / {len(video_ids)} 本処理…")

    if dfs:
        merged = pd.concat(dfs, ignore_index=True)
        cols = list(merged.columns)
        for k in ["videoId", "videoTitle", "day"]:
            if k in cols:
                cols.remove(k)
        merged = merged[["videoId", "videoTitle", "day"] + cols]
        return merged
    else:
        return pd.DataFrame(
            columns=["videoId", "videoTitle", "day"] + [m.strip() for m in
                                                        metrics.split(",")])


def videos_daily_via_analytics_only(yta, youtube_opt, *, ids, startDate,
                                    endDate, metrics,
                                    sort=None) -> pd.DataFrame:
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

    if "video" in df.columns:
        df = df.rename(columns={"video": "videoId"})
    df.insert(1, "videoTitle", "")
    if youtube_opt is not None:
        try:
            uniq_ids = sorted(set(df["videoId"].astype(str).tolist()))
            title_map = get_video_title_map(youtube_opt, uniq_ids)
            df["videoTitle"] = df["videoId"].map(
                lambda x: title_map.get(str(x), ""))
        except Exception as e:
            print(f"[WARN] タイトル取得に失敗しました（継続します）。詳細: {e}")

    cols = list(df.columns)
    for k in ["videoId", "videoTitle", "day"]:
        if k in cols:
            cols.remove(k)
    df = df[["videoId", "videoTitle", "day"] + cols]
    return df


def save_csv(df: pd.DataFrame, path: Path, mode: str = "excel"):
    """
    mode:
      - "excel"   : Excel安全（utf-8-sig, CRLF, 全列クォート）
      - "minimal" : 従来どおり（utf-8, LF, 最小クォート）
    """
    if mode == "excel":
        df.to_csv(
            path,
            index=False,
            encoding="utf-8-sig",
            lineterminator="\r\n",
            quoting=csv.QUOTE_ALL,
        )
    else:
        df.to_csv(
            path,
            index=False,
            encoding="utf-8",
            quoting=csv.QUOTE_MINIMAL,
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="YouTube Analytics: 動画×日別 + コメント本文エクスポート（reports.json不要）")
    parser.add_argument("--start", default=START_DATE_DEFAULT,
                        help="開始日 YYYY-MM-DD（デフォルト：過去90日）")
    parser.add_argument("--end", default=END_DATE_DEFAULT,
                        help="終了日 YYYY-MM-DD（デフォルト：今日）")
    parser.add_argument("--ids", default="channel==MINE",
                        help='対象IDs（例："channel==MINE" または "channel==UCxxxx"）')
    parser.add_argument("--metrics", default=DEFAULT_METRICS,
                        help="カンマ区切りのメトリクス（別名は自動マッピング）")
    parser.add_argument("--sort", default="day,video",
                        help="ソートキー（例：day,video / -day など）")
    parser.add_argument("--reauth", action="store_true",
                        help="強制再認証（token.json を作り直す）")
    parser.add_argument("--video-ids-file",
                        help="1行1IDの外部動画IDリスト。指定時は Data API を使わずこのリストで処理")
    parser.add_argument("--comments", type=int, default=50,
                        help="各動画あたり取得する最新トップレベルコメント数。0でコメント取得なし")
    parser.add_argument("--no-fallback", action="store_true",
                        help="フォールバック（analytics-only）を無効化する")
    parser.add_argument("--csv-compat", choices=["excel", "minimal"],
                        default="excel",
                        help="CSV互換モード（デフォルト: excel）")
    parser.add_argument("--no-author-channel", action="store_true",
                        help="コメントCSVに authorChannelId を含めない")
    return parser.parse_args()


def main():
    args = parse_args()

    start_date = args.start
    end_date = args.end
    ids = args.ids
    metrics = sanitize_metrics(args.metrics)
    sort = args.sort
    comments_per_video = max(args.comments or 0, 0)

    print(f"Fetching data from {start_date} to {end_date}...")

    reset_out_dir()

    required_scopes = list(SCOPES_BASE)
    if comments_per_video > 0 and SCOPE_FORCE_SSL not in required_scopes:
        required_scopes.append(SCOPE_FORCE_SSL)

    creds = get_credentials(required_scopes, force_reauth=args.reauth)
    yta = build_yt_analytics_service(creds)

    youtube_opt = None
    try:
        youtube_opt = build_yt_data_service(creds)
    except Exception as e:
        print(
            f"[WARN] YouTube Data API 初期化に失敗（タイトル/コメント付与はスキップ）。詳細: {e}")

    if args.video_ids_file:
        video_ids = [line.strip() for line in
                     Path(args.video_ids_file).read_text(
                         encoding="utf-8").splitlines() if line.strip()]
        print(
            f"外部リストから動画IDを {len(video_ids)} 件読み込みました。日別×動画で集計します…")

        title_map = {}
        if youtube_opt is not None:
            try:
                title_map = get_video_title_map(youtube_opt, video_ids)
            except Exception as e:
                print(f"[WARN] タイトル取得に失敗（継続）。詳細: {e}")

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
                df.insert(1, "videoTitle", title_map.get(vid, ""))
                dfs.append(df)
            if i % 50 == 0:
                print(f"  {i} / {len(video_ids)} 本処理…")

        metrics_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(
            columns=["videoId", "videoTitle", "day"] + [m.strip() for m in
                                                        metrics.split(",")]
        )
        if not metrics_df.empty:
            cols = list(metrics_df.columns)
            for k in ["videoId", "videoTitle", "day"]:
                if k in cols:
                    cols.remove(k)
            metrics_df = metrics_df[["videoId", "videoTitle", "day"] + cols]
    else:
        try:
            if youtube_opt is None:
                raise RuntimeError("YouTube Data API が利用できません。")
            metrics_df = videos_daily_via_loop(
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
            print(
                f"[WARN] 動画IDの取得またはループ集計に失敗。フォールバックして一括取得します。\n詳細: {e}")
            metrics_df = videos_daily_via_analytics_only(
                yta,
                youtube_opt,
                ids=ids,
                startDate=start_date,
                endDate=end_date,
                metrics=metrics,
                sort="day",
            )

    out_path_metrics = OUT_DIR / f'videos_daily_{start_date}_{end_date}.csv'
    save_csv(metrics_df, out_path_metrics, mode=args.csv_compat)
    print(f"videos_daily -> {out_path_metrics.name}")

    if comments_per_video > 0:
        if youtube_opt is None:
            print(
                "[WARN] Data API が使えないためコメント本文は取得できません。--comments は無視されます。")
        elif metrics_df.empty:
            print(
                "[INFO] メトリクス結果が空のため、コメント取得をスキップします。")
        else:
            video_ids = sorted(set(metrics_df["videoId"].astype(str).tolist()))
            try:
                title_map = get_video_title_map(youtube_opt, video_ids)
            except Exception as e:
                print(f"[WARN] タイトル再取得に失敗（継続）。詳細: {e}")
                title_map = {}

            print(
                f"コメント本文を取得します（対象動画: {len(video_ids)} 本、各 {comments_per_video} 件）。")
            comments_df = fetch_latest_comments(
                youtube_opt, video_ids, title_map,
                per_video=comments_per_video,
                since_iso=start_date,
                until_iso=end_date,
                include_author_channel=not args.no_author_channel,
            )

            out_path_comments = OUT_DIR / f'comments_{start_date}_{end_date}.csv'
            save_csv(comments_df, out_path_comments, mode=args.csv_compat)
            print(f"comments -> {out_path_comments.name}")

    zip_out_dir()


if __name__ == "__main__":
    main()
