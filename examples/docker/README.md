# Docker Example

Docker で MineSQL を起動するサンプル

## Run

```sh
# pwd -> ~/path/to/minesql
make docker-up    # コンテナ起動 + ログ表示
make docker-logs  # パスワード確認
```

## Connect

```sh
mysql -u root -p<password> -h 127.0.0.1 -P 13306 --default-auth=caching_sha2_password --ssl-mode=REQUIRED
```

## Stop

```sh
make docker-down  # コンテナ停止 + データ削除
```
