# Transaction

| 機能 | 実装 | 説明 |
| --- | --- | --- |
| Atomicity | ✅ | ROLLBACK と UNDO ログによって実現 |  
| Isolation | ✅ | 現状は Strict 2PL によって実現 |
| Durability | ✅ | REDO ログ、クラッシュリカバリによって実現 |
| トランザクション分離レベルの指定 | - | - |
| MVCC | - | - |
