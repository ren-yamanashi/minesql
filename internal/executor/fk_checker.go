package executor

import (
	"fmt"
	"minesql/internal/storage/access"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/handler"
	"minesql/internal/storage/lock"
)

var errFKDeleteRestrict = fmt.Errorf("cannot delete or update a parent row: a foreign key constraint fails")

// checkFKOnInsert は INSERT 時に参照先テーブルに値が存在するかを確認する
//
// FK 制約ごとに、参照先テーブルの PK/インデックスで FK カラムの値を検索し、
// 見つかれば Shared Lock を取得して DELETE との競合を防ぐ。
// Current Read を使用するため、同一トランザクション内の未コミット変更も可視。
func checkFKOnInsert(bp *buffer.BufferPool, trxId handler.TrxId, lockMgr *lock.Manager, tableMeta *dictionary.TableMeta, record Record) error {
	fks := tableMeta.GetForeignKeyConstraints()
	if len(fks) == 0 {
		return nil
	}

	hdl := handler.Get()
	for _, fk := range fks {
		col, ok := tableMeta.GetColByName(fk.ColName)
		if !ok {
			return fmt.Errorf("foreign key column '%s' not found in table '%s'", fk.ColName, tableMeta.Name)
		}

		fkValue := record[col.Pos]
		if err := checkRefValueExists(bp, trxId, lockMgr, hdl, fk, fkValue); err != nil {
			return err
		}
	}
	return nil
}

// checkFKOnDelete は DELETE 時に参照元テーブルからの参照がないことを確認する
//
// refFKs は呼び出し側で GetForeignKeysReferencingTable で取得した FK 制約のリスト。
// 参照元テーブルの FK カラムのインデックスで削除対象の値を検索する。
func checkFKOnDelete(bp *buffer.BufferPool, tableMeta *dictionary.TableMeta, refFKs []dictionary.ChildForeignKey, record Record) error {
	hdl := handler.Get()

	for _, childFK := range refFKs {
		// 参照先カラム (このテーブル側) の値を取得
		refCol, ok := tableMeta.GetColByName(childFK.Constraint.RefColName)
		if !ok {
			continue
		}
		refValue := record[refCol.Pos]

		// 参照元テーブルの FK カラムのインデックスで検索
		if err := checkChildRecordExists(bp, hdl, childFK, refValue); err != nil {
			return err
		}
	}
	return nil
}

// checkFKOnUpdate は UPDATE 時に FK 制約を双方向でチェックする
//
// 1. このテーブルが親テーブルとして参照されている場合、旧値で参照元テーブルを検索
// 2. このテーブルが FK を持つ場合、新値が参照先テーブルに存在するかを確認
func checkFKOnUpdate(bp *buffer.BufferPool, trxId handler.TrxId, lockMgr *lock.Manager, tableMeta *dictionary.TableMeta, refFKs []dictionary.ChildForeignKey, fks []*dictionary.ConstraintMeta, oldRecord Record, newRecord Record) error {
	hdl := handler.Get()

	// 1. 親テーブルとして: 旧値が参照されていないか確認
	for _, childFK := range refFKs {
		refCol, ok := tableMeta.GetColByName(childFK.Constraint.RefColName)
		if !ok {
			continue
		}

		// 更新対象カラムの値が変わらない場合はチェック不要
		if string(oldRecord[refCol.Pos]) == string(newRecord[refCol.Pos]) {
			continue
		}

		if err := checkChildRecordExists(bp, hdl, childFK, oldRecord[refCol.Pos]); err != nil {
			return err
		}
	}

	// 2. 子テーブルとして: 新値が参照先に存在するか確認
	for _, fk := range fks {
		col, ok := tableMeta.GetColByName(fk.ColName)
		if !ok {
			continue
		}

		// FK カラムの値が変わらない場合はチェック不要
		if string(oldRecord[col.Pos]) == string(newRecord[col.Pos]) {
			continue
		}

		if err := checkRefValueExists(bp, trxId, lockMgr, hdl, fk, newRecord[col.Pos]); err != nil {
			return err
		}
	}

	return nil
}

// checkRefValueExists は参照先テーブルに値が存在するかを確認し、Shared Lock を取得する
func checkRefValueExists(bp *buffer.BufferPool, trxId handler.TrxId, lockMgr *lock.Manager, hdl *handler.Handler, fk *dictionary.ConstraintMeta, value []byte) error {
	refTableMeta, ok := hdl.Catalog.GetTableMetaByName(fk.RefTableName)
	if !ok {
		return fmt.Errorf("referenced table '%s' not found", fk.RefTableName)
	}

	refCol, ok := refTableMeta.GetColByName(fk.RefColName)
	if !ok {
		return fmt.Errorf("referenced column '%s' not found in table '%s'", fk.RefColName, fk.RefTableName)
	}

	// 参照先カラムが PK の場合、クラスタ化インデックスで検索
	isPK := refCol.Pos < uint16(refTableMeta.PKCount)
	if isPK {
		return checkRefValueInPK(bp, trxId, lockMgr, hdl, refTableMeta, value)
	}

	// 参照先カラムが UK の場合、セカンダリインデックスで検索
	return checkRefValueInUniqueIndex(bp, trxId, lockMgr, hdl, refTableMeta, fk.RefColName, value)
}

// checkRefValueInPK はクラスタ化インデックス (PK) で値の存在を確認する
func checkRefValueInPK(bp *buffer.BufferPool, trxId handler.TrxId, lockMgr *lock.Manager, hdl *handler.Handler, refTableMeta *dictionary.TableMeta, value []byte) error {
	refTable, err := hdl.GetTable(refTableMeta.Name)
	if err != nil {
		return err
	}

	// PK でエンコードして検索
	var encodedKey []byte
	encode.Encode([][]byte{value}, &encodedKey)

	btr := btree.NewBTree(refTable.MetaPageId)
	_, pos, err := btr.FindByKey(bp, encodedKey)
	if err != nil {
		return fkConstraintError(value)
	}

	// Shared Lock を先に取得して、他トランザクションの SoftDelete 完了を待つ
	// (未コミットの SoftDelete がロールバックされればヘッダーが元に戻るため、ロック取得後に判定する)
	if err := lockMgr.Lock(trxId, pos, lock.Shared); err != nil {
		return err
	}

	// ロック取得後にレコードを再読み込みしてソフトデリート状態を確認
	record, _, err := btr.FindByKey(bp, encodedKey)
	if err != nil {
		return fkConstraintError(value)
	}
	if record.HeaderBytes()[0] == 1 {
		return fkConstraintError(value)
	}

	return nil
}

// checkRefValueInUniqueIndex はセカンダリインデックス (UK) で値の存在を確認する
func checkRefValueInUniqueIndex(bp *buffer.BufferPool, trxId handler.TrxId, lockMgr *lock.Manager, hdl *handler.Handler, refTableMeta *dictionary.TableMeta, refColName string, value []byte) error {
	refTable, err := hdl.GetTable(refTableMeta.Name)
	if err != nil {
		return err
	}

	// セカンダリインデックスを取得
	var si *access.SecondaryIndex
	for _, idx := range refTable.SecondaryIndexes {
		if idx.ColName == refColName {
			si = idx
			break
		}
	}
	if si == nil {
		return fmt.Errorf("unique index for column '%s' not found", refColName)
	}

	// セカンダリキーでエンコードして検索
	var encodedSecKey []byte
	encode.Encode([][]byte{value}, &encodedSecKey)

	btr := btree.NewBTree(si.MetaPageId)
	iter, err := btr.Search(bp, btree.SearchModeKey{Key: encodedSecKey})
	if err != nil {
		return fkConstraintError(value)
	}

	// セカンダリキーが一致する active レコードを探す
	for {
		record, ok := iter.Get()
		if !ok {
			return fkConstraintError(value)
		}

		var keyColumns [][]byte
		encode.Decode(record.KeyBytes(), &keyColumns)
		if len(keyColumns) == 0 {
			return fkConstraintError(value)
		}

		var existingSecKey []byte
		encode.Encode(keyColumns[:1], &existingSecKey)
		if len(existingSecKey) != len(encodedSecKey) || string(existingSecKey) != string(encodedSecKey) {
			return fkConstraintError(value)
		}

		// Shared Lock を先に取得して、他トランザクションの SoftDelete 完了を待つ
		if err := lockMgr.Lock(trxId, iter.LastPosition, lock.Shared); err != nil {
			return err
		}

		// ロック取得後にソフトデリート状態を確認
		if record.HeaderBytes()[0] != 1 {
			return nil
		}

		if err := iter.Advance(bp); err != nil {
			return fkConstraintError(value)
		}
	}
}

// checkChildRecordExists は参照元テーブルに指定された値を参照するレコードが存在するかチェックする
//
// 存在する場合はエラーを返す (RESTRICT)
func checkChildRecordExists(bp *buffer.BufferPool, hdl *handler.Handler, childFK dictionary.ChildForeignKey, value []byte) error {
	childTable, err := hdl.GetTable(childFK.TableName)
	if err != nil {
		return err
	}

	// FK カラムのインデックスで検索
	var si *access.SecondaryIndex
	for _, idx := range childTable.SecondaryIndexes {
		if idx.ColName == childFK.Constraint.ColName {
			si = idx
			break
		}
	}
	if si == nil {
		return fmt.Errorf("index for foreign key column '%s' not found in table '%s'", childFK.Constraint.ColName, childFK.TableName)
	}

	var encodedSecKey []byte
	encode.Encode([][]byte{value}, &encodedSecKey)

	btr := btree.NewBTree(si.MetaPageId)
	iter, err := btr.Search(bp, btree.SearchModeKey{Key: encodedSecKey})
	if err != nil {
		// 検索に失敗した場合、参照元レコードは存在しないとみなす
		return nil //nolint:nilerr
	}

	for {
		record, ok := iter.Get()
		if !ok {
			return nil
		}

		var keyColumns [][]byte
		encode.Decode(record.KeyBytes(), &keyColumns)
		if len(keyColumns) == 0 {
			return nil
		}

		var existingSecKey []byte
		encode.Encode(keyColumns[:1], &existingSecKey)
		if len(existingSecKey) != len(encodedSecKey) || string(existingSecKey) != string(encodedSecKey) {
			return nil
		}

		// active レコードが存在する → RESTRICT
		if record.HeaderBytes()[0] != 1 {
			return errFKDeleteRestrict
		}

		if err := iter.Advance(bp); err != nil {
			// イテレータの進行に失敗した場合、参照元レコードは存在しないとみなす
			return nil //nolint:nilerr
		}
	}
}

// fkConstraintError は FK 制約違反のエラーメッセージを生成する
func fkConstraintError(value []byte) error {
	return fmt.Errorf("cannot add or update a child row: a foreign key constraint fails (value '%s')", string(value))
}
