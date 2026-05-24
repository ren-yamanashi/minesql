package access

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var ErrForeignKeyViolation = errors.New("access: foreign key constraint violation")

// checkForeignKeysForInsert は Insert 前に FK 制約チェックを行う
//
// 自テーブルが FK を持つ場合、挿入する値が参照先テーブルの PK に存在するか確認する
func (t *Table) checkForeignKeysForInsert(colNames, values []string) error {
	fks, err := fetchForeignKeys(t.catalog, t.primaryIndex.fileId())
	if err != nil {
		return err
	}
	if len(fks) == 0 {
		return nil
	}

	valMap := t.buildValMap(colNames, values)
	for _, fk := range fks {
		if err := checkParentRecordExists(
			t.bufferPool,
			t.catalog,
			fk.ReferenceTableFileId(),
			valMap[fk.ColumnName()],
		); err != nil {
			return err
		}
	}
	return nil
}

// checkForeignKeysForDelete は SoftDelete 前に FK 制約チェックを行う
//
// 自テーブルが親テーブルとして参照されている場合に、
// 削除する値が参照元テーブルから参照されていないか確認する
func (t *Table) checkForeignKeysForDelete(record *PrimaryRecord) error {
	refs, err := fetchReferencingConstraints(t.catalog, t.primaryIndex.fileId())
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}

	valMap := t.buildValMap(record.colNames, record.values)
	for _, ref := range refs {
		if err := checkChildRecordExists(
			t.bufferPool,
			t.catalog,
			ref,
			valMap[ref.ReferenceColumnName()],
		); err != nil {
			return err
		}
	}
	return nil
}

// checkForeignKeysForUpdate は Update (インプレース) 前に FK 制約チェックを行う
//
// FK に関係するカラムの値が変わった制約のみチェックする
//   - 親テーブルチェック: 旧値が参照元から参照されていないか
//   - 子テーブルチェック: 新値が参照先に存在するか
func (t *Table) checkForeignKeysForUpdate(before, after *PrimaryRecord) error {
	if err := t.checkParentRefsForUpdate(before, after); err != nil {
		return err
	}
	return t.checkChildRefsForUpdate(before, after)
}

// checkParentRefsForUpdate は自テーブルを参照する FK の参照先カラム値が変わった場合に、
// 旧値が参照元から参照されていないかを確認する
func (t *Table) checkParentRefsForUpdate(before, after *PrimaryRecord) error {
	refs, err := fetchReferencingConstraints(t.catalog, t.primaryIndex.fileId())
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}

	beforeMap := t.buildValMap(before.colNames, before.values)
	afterMap := t.buildValMap(after.colNames, after.values)
	for _, ref := range refs {
		colName := ref.ReferenceColumnName()
		if beforeMap[colName] == afterMap[colName] {
			continue
		}
		if err := checkChildRecordExists(
			t.bufferPool,
			t.catalog,
			ref,
			beforeMap[colName],
		); err != nil {
			return err
		}
	}
	return nil
}

// checkChildRefsForUpdate は自テーブルの FK カラムの値が変わった場合に、
// 新値が参照先テーブルの PK に存在するかを確認する。
func (t *Table) checkChildRefsForUpdate(before, after *PrimaryRecord) error {
	fks, err := fetchForeignKeys(t.catalog, t.primaryIndex.fileId())
	if err != nil {
		return err
	}
	if len(fks) == 0 {
		return nil
	}

	beforeMap := t.buildValMap(before.colNames, before.values)
	afterMap := t.buildValMap(after.colNames, after.values)
	for _, fk := range fks {
		colName := fk.ColumnName()
		if beforeMap[colName] == afterMap[colName] {
			continue
		}
		if err := checkParentRecordExists(
			t.bufferPool,
			t.catalog,
			fk.ReferenceTableFileId(),
			afterMap[colName],
		); err != nil {
			return err
		}
	}
	return nil
}

// fetchForeignKeys は自テーブル (子テーブル) の FK 制約一覧を返す
func fetchForeignKeys(ct *catalog.Catalog, fileId page.FileId) ([]catalog.ConstraintRecord, error) {
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := ct.ConstraintMeta().Search(catalog.SearchModeKey{Key: [][]byte{fileIdBytes}})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var fks []catalog.ConstraintRecord
	for {
		record, ok, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok || record.FileId() != fileId {
			break
		}
		fks = append(fks, record)
	}
	return fks, nil
}

// fetchReferencingConstraints は自テーブルを親として参照している FK 制約一覧を返す
func fetchReferencingConstraints(ct *catalog.Catalog, fileId page.FileId) ([]catalog.ConstraintRecord, error) {
	iter, err := ct.ConstraintMeta().Search(catalog.SearchModeStart{})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var refs []catalog.ConstraintRecord
	for {
		record, ok, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if record.ReferenceTableFileId() == fileId {
			refs = append(refs, record)
		}
	}
	return refs, nil
}

// checkParentRecordExists は参照先テーブルの PK に値が存在するか確認する
func checkParentRecordExists(
	bp *buffer.Pool,
	ct *catalog.Catalog,
	refFileId page.FileId,
	value string,
) error {
	indexRecord, err := fetchPrimaryIndexRecord(ct, refFileId)
	if err != nil {
		return err
	}

	tree := btree.NewTree(bp, indexRecord.MetaPageId())
	var sk []byte
	encode.Encode([][]byte{[]byte(value)}, &sk)

	record, _, err := tree.FindByKey(sk)
	if errors.Is(err, btree.ErrKeyNotFound) {
		return ErrForeignKeyViolation
	}
	if err != nil {
		return err
	}
	if record.Header()[0] == 1 {
		return ErrForeignKeyViolation
	}
	return nil
}

// checkChildRecordExists は参照元テーブルの FK カラムに対応するセカンダリインデックスから
// active なレコードを検索し、1 件でも存在すれば FK 違反エラーを返す。
func checkChildRecordExists(
	bp *buffer.Pool,
	ct *catalog.Catalog,
	constraint catalog.ConstraintRecord,
	value string,
) error {
	indexRecord, err := findFKSecondaryIndex(ct, constraint)
	if err != nil {
		return err
	}
	return hasActiveChildRecord(bp, indexRecord, value)
}

// findFKSecondaryIndex は FK カラムが先頭カラム (position 0) として含まれるセカンダリインデックスを返す
//
// FK カラムがインデックスの先頭でない場合、prefix 検索で正しく該当レコードを抽出できないため除外する。
func findFKSecondaryIndex(
	ct *catalog.Catalog,
	constraint catalog.ConstraintRecord,
) (catalog.IndexRecord, error) {
	records, err := fetchSecondaryIndexRecords(ct, constraint.FileId())
	if err != nil {
		return catalog.IndexRecord{}, err
	}
	for _, indexRecord := range records {
		keyCols, err := fetchIndexKeyColumn(ct, indexRecord.IndexId())
		if err != nil {
			return catalog.IndexRecord{}, err
		}
		if pos, ok := keyCols[constraint.ColumnName()]; ok && pos == 0 {
			return indexRecord, nil
		}
	}
	return catalog.IndexRecord{}, fmt.Errorf(
		"access: no secondary index found for foreign key column %q",
		constraint.ColumnName(),
	)
}

// hasActiveChildRecord は指定したセカンダリインデックスで value を先頭キーに持つ active なレコードが存在するかを確認する
func hasActiveChildRecord(
	bp *buffer.Pool,
	indexRecord catalog.IndexRecord,
	value string,
) error {
	tree := btree.NewTree(bp, indexRecord.MetaPageId())
	var sk []byte
	encode.Encode([][]byte{[]byte(value)}, &sk)

	iter, err := tree.Search(btree.SearchModeKey{Key: sk})
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		existing, ok, err := iter.Get()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if !bytes.HasPrefix(existing.Key(), sk) {
			return nil
		}
		if existing.Header()[0] != 1 {
			return ErrForeignKeyViolation
		}
		if err := iter.Advance(); err != nil {
			return err
		}
	}
}
