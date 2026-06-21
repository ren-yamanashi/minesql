package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type CreateConstraintInput struct {
	ColumnName          string // 制約のあるカラム名
	ConstraintName      string // 制約名
	ReferenceTableName  string // 制約により参照されるテーブル名
	ReferenceColumnName string // 制約により参照されるカラム名 (構成順通り)
}

type CreateIndexInput struct {
	IndexName string               // インデックス名
	ColNames  []string             // インデックスを構成するカラム名 (構成順通り)
	IndexType dictionary.IndexType // インデックス種類
}

type CreateTableInput struct {
	TableName   string                  // テーブル名
	ColNames    []string                // カラム名のリスト
	PkCount     int                     // プライマリキーのカラム数
	Indexes     []CreateIndexInput      // インデックス
	Constraints []CreateConstraintInput // 制約
}

// CreateTable はテーブルを新規作成する
//   - tm: DDL Transaction を払い出すための TrxManager。 配下の catalog / bufferPool / redoLog / lockMgr / undoLog を共有する
//   - 各 step は DDLReservedTrxId 配下の mtr で「データ変更 + DDL Undo 書き込み」を 1 つの mtr で原子的に記録する
//   - 途中失敗時は defer 経由で tm.Rollback(ddlTrx) を呼び、 DDL Undo を逆順適用して完全クリーンアップする
func CreateTable(tm *TrxManager, input CreateTableInput) (table *Table, err error) {
	ddlTrx := tm.BeginDDL()
	defer func() {
		if err != nil {
			_ = tm.Rollback(ddlTrx)
		}
	}()

	bp := tm.bufferPool
	redoLog := tm.redoLog
	lockMgr := tm.lock
	undoLog := tm.undoLog
	ct := tm.catalog

	// ファイル作成 + AllocateFileIdUndo
	fileId, err := createTableFile(ct, bp, redoLog, input.TableName)
	if err != nil {
		return nil, err
	}

	// プライマリインデックス作成 + CreateBTreeUndo
	pi, err := createPrimaryIndex(ct, bp, fileId, input.PkCount, lockMgr, undoLog, redoLog)
	if err != nil {
		return nil, err
	}

	// テーブルメタ・インデックスメタ・カラムメタの登録 + MetaInsertUndo
	registerMtr := buffer.NewWriteMtr(bp, lock.DDLReservedTrxId, redoLog)
	if err = registerTableMeta(registerMtr, ct, fileId, pi, input); err != nil {
		registerMtr.UnpinAll()
		return nil, err
	}
	if err = registerMtr.Commit(); err != nil {
		return nil, err
	}

	// セカンダリインデックス作成 + 各種 DDL Undo (= 各セカンダリ 1 mtr で atomic)
	sis, err := createSecondaryIndexes(ct, bp, fileId, pi.tree, lockMgr, undoLog, redoLog, input.Indexes)
	if err != nil {
		return nil, err
	}

	// 制約登録 + MetaInsertUndo
	constraintsMtr := buffer.NewWriteMtr(bp, lock.DDLReservedTrxId, redoLog)
	if err = createConstraints(constraintsMtr, ct, bp, fileId, input.Constraints); err != nil {
		constraintsMtr.UnpinAll()
		return nil, err
	}
	if err = constraintsMtr.Commit(); err != nil {
		return nil, err
	}

	if err = tm.Commit(ddlTrx); err != nil {
		return nil, err
	}

	table = &Table{
		primaryIndex:     pi,
		secondaryIndexes: sis,
		catalog:          ct,
		undoLog:          undoLog,
		lock:             lockMgr,
		bufferPool:       bp,
		redoLog:          redoLog,
	}
	return table, nil
}

// appendMetaInsertUndo はカタログ Meta テーブルへの 1 件の Insert を取り消すための MetaInsertUndo を Append する
func appendMetaInsertUndo(mtr *buffer.Mtr, ct *dictionary.Catalog, metaTableType undo.MetaTableType, key []byte) error {
	record := undo.NewDDLRecord(
		undo.DDLRecordTypeMetaInsert,
		undo.NewMetaInsertUndoRecord(metaTableType, key).Serialize(),
	)
	return ct.DDLManager().Append(mtr, record)
}
