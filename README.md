# youtube-analytics-exporter

YouTube Analytics API を使って各種レポートを一括ダウンロードするPythonスクリプトです。

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

### YouTube Analytics API
- https://developers.google.com/youtube/analytics/dimensions
- https://developers.google.com/youtube/analytics/metrics
