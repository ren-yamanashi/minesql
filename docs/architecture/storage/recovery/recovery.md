# クラッシュリカバリ

## 概要

- 異常終了時、バッファプール上のダーティーページはディスクに反映されていない可能性がある
- クラッシュリカバリは、異常終了後の再起動時にディスク上のデータを整合性のある状態に復元する処理
- REDO ログの適用でコミット済みの変更を復元し、UNDO ログの適用で未完了の変更を取り消す
- [チェックポイント](../access/checkpoint.md) LSN を起点とすることで、走査範囲を限定する

## リカバリの流れ

```mermaid
flowchart TD
    Start[サーバー起動] --> ReadCP[REDO ログヘッダーから checkpoint LSN を読み取る]
    ReadCP --> ReadRec[checkpoint LSN 以降の REDO レコードを読み込む]
    ReadRec --> HasRec{REDO レコードが存在するか}
    HasRec -- 存在しない --> Clean[正常終了済み <br/> リカバリ不要]
    HasRec -- 存在する --> Redo[REDO 適用]

    subgraph Redo [REDO 適用]
        ScanRec[REDO レコードを先頭から走査]
        ScanRec --> IsPageWrite{ページ変更レコードか}
        IsPageWrite -- "COMMIT / ROLLBACK" --> NextRec
        IsPageWrite -- ページ変更 --> CompareLSN{Page LSN ≥ レコード LSN か}
        CompareLSN -- "はい (適用済み)" --> NextRec[次のレコードへ]
        CompareLSN -- いいえ --> Apply[ページをレコードの内容で上書き]
        Apply --> NextRec
        NextRec --> MoreRec{次のレコードがあるか}
        MoreRec -- ある --> IsPageWrite
    end

    MoreRec -- ない --> Undo[UNDO ロールバック]

    subgraph Undo [UNDO ロールバック]
        FindActive[REDO レコードからトランザクション一覧を取得]
        FindActive --> FindCompleted[COMMIT / ROLLBACK レコードがあるトランザクションを除外]
        FindCompleted --> HasUncommitted{未完了のトランザクションがあるか}
        HasUncommitted -- ない --> UndoDone[ロールバック完了]
        HasUncommitted -- ある --> ScanUndo[UNDO ページを走査して該当トランザクションのレコードを収集]
        ScanUndo --> ReverseApply[UNDO レコードを逆順に適用]
        ReverseApply --> UndoDone
    end

    UndoDone --> FlushAll[全ダーティーページをディスクにフラッシュ]
    FlushAll --> ClearRedo[REDO ログをクリア]
    ClearRedo --> Done[リカバリ完了]
```
