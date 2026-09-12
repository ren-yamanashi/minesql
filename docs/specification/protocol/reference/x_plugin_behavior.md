# X Plugin のメッセージ処理の振る舞い (補足)

- [message_spec.md](../message_spec.md) から分けた、X Plugin の実装の振る舞いの記録 (実測値、doc コメントと実装の差、状態ごとの例外的な応答)
- `.proto` ファイルが定める契約ではないが、クライアントはこの振る舞いも前提にしうるため、minesql も互換の要件として同じ応答をする
- 実装時の参照資料で、読む順には含めない

## capability の実測値

- 下の実測は MySQL 8.0.39 のものだが、8.4 のソースでも capability の構成は同じ 8 個
  - そのうち `session_connect_attrs` は設定専用 (取得不可) のため、一覧には現れない
  - `authentication.mechanisms` の値は安全な接続 (TLS または Unix ソケット) かどうかで変わる
  - 参照:
    - [client.cc の capabilities_configurator](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L199-L221)
    - [handler_connection_attributes.h](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/capabilities/handler_connection_attributes.h#L37-L39)

<details><summary>実測: CapabilitiesGet のレスポンス (MySQL 8.0.39、TCP の非 TLS 接続)</summary>

```txt
capabilities {
  name: "tls"
  value {
    type: SCALAR
    scalar {
      type: V_BOOL
      v_bool: false
    }
  }
}
capabilities {
  name: "authentication.mechanisms"
  value {
    type: ARRAY
    array {
      value {
        type: SCALAR
        scalar {
          type: V_STRING
          v_string {
            value: "MYSQL41"
          }
        }
      }
      value {
        type: SCALAR
        scalar {
          type: V_STRING
          v_string {
            value: "SHA256_MEMORY"
          }
        }
      }
    }
  }
}
capabilities {
  name: "doc.formats"
  value {
    type: SCALAR
    scalar {
      type: V_STRING
      v_string {
        value: "text"
      }
    }
  }
}
capabilities {
  name: "client.interactive"
  value {
    type: SCALAR
    scalar {
      type: V_BOOL
      v_bool: false
    }
  }
}
capabilities {
  name: "compression"
  value {
    type: OBJECT
    obj {
      fld {
        key: "algorithm"
        value {
          type: ARRAY
          array {
            value {
              type: SCALAR
              scalar {
                type: V_STRING
                v_string {
                  value: "deflate_stream"
                }
              }
            }
            value {
              type: SCALAR
              scalar {
                type: V_STRING
                v_string {
                  value: "lz4_message"
                }
              }
            }
            value {
              type: SCALAR
              scalar {
                type: V_STRING
                v_string {
                  value: "zstd_stream"
                }
              }
            }
          }
        }
      }
    }
  }
}
capabilities {
  name: "node_type"
  value {
    type: SCALAR
    scalar {
      type: V_STRING
      v_string {
        value: "mysql"
      }
    }
  }
}
capabilities {
  name: "client.pwd_expire_ok"
  value {
    type: SCALAR
    scalar {
      type: V_BOOL
      v_bool: false
    }
  }
}
```
```

</details>

## 公式クライアントの認証メカニズムの自動選択

- プロトコルとしての既定のメカニズムはない (`mech_name` は required フィールドで、クライアントが必ず明示する)
- 公式クライアントライブラリでメカニズムを指定しなかった場合は自動選択 (`AUTO`) になり、安全な接続 (TLS または Unix ソケット) なら `SHA256_MEMORY` → `PLAIN` → `MYSQL41`、そうでなければ `SHA256_MEMORY` → `MYSQL41` の順に成功するまで試行する
- 参照:
  - [plugin/x/client/xsession_impl.cc の `get_methods_sequence_from_auto`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/client/xsession_impl.cc#L969-L996)
  - [同ファイルの安全な接続の判定](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/client/xsession_impl.cc#L877-L879)

## 再認証待ちの接続が受け付けるメッセージ

- `Session.Close` のあと、接続は再認証待ちの状態に戻る
- この状態で `AuthenticateStart` / `AuthenticateContinue` 以外のメッセージを送ると、`Connection.Close` であっても FATAL の `Error` (code 5000 "Invalid message") が返り接続が切断される
  - 初回の認証前 (接続直後) とは扱いが異なり、そちらでは `Connection.Close` に `Ok` ("bye!") が返って正常に閉じる
  - 参照:
    - [session.cc の handle_auth_message](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L141-L202)
    - [client.cc の handle_message (初回認証前の処理)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L264-L337)
- 例外として、`session_connect_attrs` capability のみを含む `CapabilitiesSet` はこの状態でも受け付けられる (他の capability を含むと FATAL の `Error` が返る)
  - 参照:
    - [client.cc の handle_session_connect_attr_set](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L244-L262)

## doc コメントと実装の差

- `INSERT` へのレスポンス
  - mysqlx_resultset.proto の doc コメントは「`INSERT` は通常リザルトセットを送らず `FetchDone` のみになる」としている
  - 実装はリザルトセットのメタデータを送った場合にしか `FetchDone` を送らないため、実際の `INSERT` へのレスポンスは Notice と `StmtExecuteOk` だけで `FetchDone` は含まれない
  - 参照:
    - [mysqlx_resultset.proto の doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_resultset.proto#L90-L94)
    - [streaming_command_delegate.cc の start_result_metadata](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L125-L138)
    - [handle_ok](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L503-L523)
- `ColumnMetaData.catalog`
  - `.proto` の doc コメントは「設定されることを期待するな」と書いているが、実装は `compact_metadata` でない場合に固定値 `"def"` を設定する
  - 参照:
    - [mysqlx_resultset.proto の catalog](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_resultset.proto#L526-L530)
    - [streaming_command_delegate.cc の field_metadata](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L309-L313)

## 不正なメッセージへの応答

- 長さ 0 のフレーム (ペイロードなし) には FATAL の `Error` (`ER_X_BAD_MESSAGE` "Messages without payload are not supported") が返り、接続が切断される (認証前の不正なメッセージと同じ扱い)
  - 参照:
    - [protocol_decoder.cc の read_and_decode_impl](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/protocol_decoder.cc#L154-L157)
    - [client.cc の run (デコードエラーを FATAL で返して切断)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L617-L624)
- 認証前に送った `Session.Reset` は無視される
  - 参照:
    - [client.cc の handle_message](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L296-L299)
- 未知の種別、または今の状態で扱えない種別のメッセージへのレスポンスは、認証の前後で異なる
  - 認証前: FATAL の `Error` (code 5000 `ER_X_BAD_MESSAGE` "Invalid message") が返り、接続が切断される
  - 認証後: 通常の `Error` (code 1047 `ER_UNKNOWN_COM_ERROR` "Unexpected message received") が返り、セッションは継続する
  - 参照:
    - [client.cc の handle_message](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L264-L337)
    - [xpl_dispatcher.cc の dispatch](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/xpl_dispatcher.cc#L118-L119)
- 認証後に送った `CapabilitiesSet` も同じく通常の `Error` (code 1047) で拒否される
  - 参照:
    - [xpl_dispatcher.cc の dispatch](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/xpl_dispatcher.cc#L46-L119)
