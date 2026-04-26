# ALTER USER の動作確認

## 前提

- サーバーをビルド済みであること
- 既存のデータをクリアした状態で確認すること

## 準備

```sh
make build
rm -rf data
```

## ケース 1: 初回起動でランダムパスワードがログに出力される

### 手順

```sh
./bin/server -h localhost -p 18888 --init-user root --init-host % &
```

### 期待結果

- サーバーログに `created with password: <ランダムパスワード>` が出力される

## ケース 2: ログに出力されたパスワードで接続できる

### 手順

```sh
mysql -u root -p<ログに出力されたパスワード> -h 127.0.0.1 -P 18888 --default-auth=caching_sha2_password
```

### 期待結果

- 接続に成功する

## ケース 3: ALTER USER でパスワードを変更できる

### 手順

接続した状態で:

```sql
ALTER USER 'root'@'%' IDENTIFIED BY 'mynewpass';
```

### 期待結果

- `Query OK` が返る

## ケース 4: 新しいパスワードで接続できる

### 手順

一度切断し、新しいパスワードで再接続:

```sh
mysql -u root -pmynewpass -h 127.0.0.1 -P 18888 --default-auth=caching_sha2_password
```

### 期待結果

- 接続に成功する

## ケース 5: 古いパスワードでは接続が拒否される

### 手順

初回起動時のランダムパスワードで接続を試行:

```sh
mysql -u root -p<古いパスワード> -h 127.0.0.1 -P 18888 --default-auth=caching_sha2_password
```

### 期待結果

- `Access denied` で接続が拒否される

## ケース 6: サーバー再起動後も新しいパスワードで接続できる

### 手順

サーバーを停止して再起動:

```sh
kill $(lsof -ti:18888)
./bin/server -h localhost -p 18888 &
```

新しいパスワードで接続:

```sh
mysql -u root -pmynewpass -h 127.0.0.1 -P 18888 --default-auth=caching_sha2_password
```

### 期待結果

- 接続に成功する (パスワード変更がカタログに永続化されている)
