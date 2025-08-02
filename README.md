# youtube-analytics-exporter

YouTube Analytics API を使って各種レポートを一括ダウンロードするPythonスクリプトです。

## 特徴
- OAuth2で安全にアナリティクスデータをCSVダウンロード
- 日次集計や複合ディメンションを含む多様なレポートに対応
- レポートの設定はJSONファイルで簡単に管理

## 使い方
### 認証情報の設定（初回のみ）
1. Google Cloud Consoleでプロジェクトを作成し、YouTube Analytics APIを有効化 
2. OAuth2クライアントIDを作成し、`client_secrets.json`として保存
3. `python main.py`を実行する
   - 初回実行時にブラウザが開き、Googleアカウントで認証
   - 認証後、`token.json`が生成され、以降は自動で使用されます

### レポートの実行
```shell
python main.py
```
または
```shell
python main.py --reports reports.json --start 2025-05-01 --end 2025-07-31 --ids "channel==MINE"
```
- 
- レポートは `yt_analytics_data/` フォルダにCSV形式で保存されます
- 必要に応じて、`reports.json` を編集して、取得したいレポートを追加できます
- CSVはzip圧縮され、`yt_analytics_data.zip` に保存されます

### 設定済みレポート
reports.jsonに定義されているレポートは以下の通りです。必要に応じて追加・変更が可能です。

| レポート名 | 説明 |
| --- | --- |
videos | 動画x日毎の統計情報（再生数、視聴時間、平均視聴時間、いいね数、コメント数、シェア数、登録者数の増減）

### YouTube Analytics API
- https://developers.google.com/youtube/analytics/dimensions
- https://developers.google.com/youtube/analytics/metrics
