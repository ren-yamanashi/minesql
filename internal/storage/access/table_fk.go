package access

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var ErrForeignKeyViolation = errors.New("access: foreign key constraint violation")

// checkForeignKeysForInsert は Insert 前に FK 制約チェックを行う
//
// 自テーブルが FK を持つ場合、挿入する値が参照先テーブルの PK に存在するか確認する
func (t *Table) checkForeignKeysForInsert(parentTrx *Transaction, colNames, values []string) error {
	fks, err := fetchForeignKeys(parentTrx, t.primaryIndex.fileId())
	if err != nil {
		return err
	}
	if len(fks) == 0 {
		return nil
	}

	valMap := t.buildValMap(colNames, values)
	for _, fk := range fks {
		if err := t.checkParentRecordExists(
			parentTrx,
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
//   - parentTrx は子側セカンダリレコードに対する共有ロック取得に使う
func (t *Table) checkForeignKeysForDelete(parentTrx *Transaction, record *PrimaryRecord) error {
	refs, err := fetchReferencingConstraints(parentTrx, t.primaryIndex.fileId())
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}

	valMap := t.buildValMap(record.colNames, record.values)
	for _, ref := range refs {
		if err := checkChildRecordExists(
			parentTrx,
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
func (t *Table) checkForeignKeysForUpdate(parentTrx *Transaction, before, after *PrimaryRecord) error {
	if err := t.checkParentRefsForUpdate(parentTrx, before, after); err != nil {
		return err
	}
	return t.checkChildRefsForUpdate(parentTrx, before, after)
}

// checkParentRefsForUpdate は自テーブルを参照する FK の参照先カラム値が変わった場合に、
// 旧値が参照元から参照されていないかを確認する
//   - parentTrx は子側セカンダリレコードに対する共有ロック取得に使う
func (t *Table) checkParentRefsForUpdate(parentTrx *Transaction, before, after *PrimaryRecord) error {
	refs, err := fetchReferencingConstraints(parentTrx, t.primaryIndex.fileId())
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
			parentTrx,
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
func (t *Table) checkChildRefsForUpdate(parentTrx *Transaction, before, after *PrimaryRecord) error {
	fks, err := fetchForeignKeys(parentTrx, t.primaryIndex.fileId())
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
		if err := t.checkParentRecordExists(
			parentTrx,
			fk.ReferenceTableFileId(),
			afterMap[colName],
		); err != nil {
			return err
		}
	}
	return nil
}

// fetchForeignKeys は自テーブル (子テーブル) の FK 制約一覧を返す
func fetchForeignKeys(parentTrx *Transaction, fileId page.FileId) ([]dictionary.ConstraintMetaRecord, error) {
	mtr := parentTrx.NewReadMtr()
	defer mtr.UnpinAll()
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := parentTrx.catalog.ConstraintMeta().Search(mtr, dictionary.SearchModeKey{Key: [][]byte{fileIdBytes}})
	if err != nil {
		return nil, err
	}

	var fks []dictionary.ConstraintMetaRecord
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
func fetchReferencingConstraints(parentTrx *Transaction, fileId page.FileId) ([]dictionary.ConstraintMetaRecord, error) {
	mtr := parentTrx.NewReadMtr()
	defer mtr.UnpinAll()
	iter, err := parentTrx.catalog.ConstraintMeta().Search(mtr, dictionary.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var refs []dictionary.ConstraintMetaRecord
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

// checkParentRecordExists は参照先 (親) テーブルの PK に値が存在するか確認する
//   - 親 PK の行ロック識別子で共有ロックを取得してから、ロック保持下で存在 (削除済みでないこと) を確認する
//   - 共有ロックは B+Tree のラッチ・Pin を保持しない状態で取得する (TOCTOU を避けるためロック取得を存在確認より前に行う)
func (t *Table) checkParentRecordExists(parentTrx *Transaction, refFileId page.FileId, value string) error {
	indexRecord, err := fetchPrimaryIndexRecord(parentTrx.catalog, parentTrx.bufferPool, refFileId)
	if err != nil {
		return err
	}

	// 親 PK で共有ロックを取得する (この時点で B+Tree のラッチ・Pin は未取得)
	sk := encode.Encode(nil, [][]byte{[]byte(value)})
	rowKey := lock.RowKey{MetaPageId: indexRecord.MetaPageId(), Key: sk}
	if err := parentTrx.lockMgr.Lock(parentTrx.trxId, rowKey, lock.Shared); err != nil {
		return err
	}

	// 共有ロック保持下で親 B+Tree を検索し、存在し削除済みでないことを確認する
	mtr := parentTrx.NewReadMtr()
	defer mtr.UnpinAll()
	tree := btree.NewTree(parentTrx.bufferPool, indexRecord.MetaPageId())
	record, _, err := tree.FindByKey(mtr, sk)
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
//   - 各子側セカンダリレコードに parentTrx の共有ロックを取り、ロック取得後の最新状態で再評価する
func checkChildRecordExists(
	parentTrx *Transaction,
	constraint dictionary.ConstraintMetaRecord,
	value string,
) error {
	indexRecord, err := findFKSecondaryIndex(parentTrx, constraint)
	if err != nil {
		return err
	}
	return hasActiveChildRecord(parentTrx, indexRecord, value)
}

// findFKSecondaryIndex は FK カラムが先頭カラム (position 0) として含まれるセカンダリインデックスを返す
//
// FK カラムがインデックスの先頭でない場合、prefix 検索で正しく該当レコードを抽出できないため除外する。
func findFKSecondaryIndex(
	parentTrx *Transaction,
	constraint dictionary.ConstraintMetaRecord,
) (dictionary.IndexMetaRecord, error) {
	records, err := fetchSecondaryIndexRecords(parentTrx.catalog, parentTrx.bufferPool, constraint.FileId())
	if err != nil {
		return dictionary.IndexMetaRecord{}, err
	}
	for _, indexRecord := range records {
		keyCols, err := fetchIndexKeyColumn(parentTrx.catalog, parentTrx.bufferPool, indexRecord.IndexId())
		if err != nil {
			return dictionary.IndexMetaRecord{}, err
		}
		if pos, ok := keyCols[constraint.ColumnName()]; ok && pos == 0 {
			return indexRecord, nil
		}
	}
	return dictionary.IndexMetaRecord{}, fmt.Errorf(
		"access: no secondary index found for foreign key column %q",
		constraint.ColumnName(),
	)
}

// hasActiveChildRecord は指定したセカンダリインデックスで value を先頭キーに持つ active なレコードが存在するかを確認する
//   - 候補キーを列挙したあと latch / Pin をすべて解放し、 各候補に対して共有ロックを取得して別 mtr で再確認する
//   - latch 保持中に lockMgr.Lock を呼ばない構造により、 他トランザクションの latch 取得を妨げない
func hasActiveChildRecord(
	parentTrx *Transaction,
	indexRecord dictionary.IndexMetaRecord,
	value string,
) error {
	sk := encode.Encode(nil, [][]byte{[]byte(value)})

	candidateKeys, err := collectCandidateKeys(parentTrx, indexRecord, sk)
	if err != nil {
		return err
	}

	for _, candidateKey := range candidateKeys {
		if err := verifyChildRecord(parentTrx, indexRecord, candidateKey); err != nil {
			return err
		}
	}
	return nil
}

// collectCandidateKeys は indexRecord で sk を prefix に持つ全 key を列挙する
//   - 関数完了時に latch / Pin はすべて解放される
//   - 返却される key はバッファページ参照ではなくコピーされたバイト列
func collectCandidateKeys(
	parentTrx *Transaction,
	indexRecord dictionary.IndexMetaRecord,
	sk []byte,
) ([][]byte, error) {
	mtr := parentTrx.NewReadMtr()
	defer mtr.UnpinAll()

	tree := btree.NewTree(parentTrx.bufferPool, indexRecord.MetaPageId())
	iter, err := tree.Search(mtr, btree.SearchModeKey{Key: sk})
	if err != nil {
		return nil, err
	}

	var keys [][]byte
	for {
		existing, ok, err := iter.Get()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if !bytes.HasPrefix(existing.Key(), sk) {
			break
		}
		keyCopy := append([]byte(nil), existing.Key()...)
		keys = append(keys, keyCopy)
		if err := iter.Advance(); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// verifyChildRecord は candidateKey に対して共有ロックを取得し、 別 mtr で最新状態を確認する
//   - 取得待ち中に物理削除されている場合は nil を返してスキップ扱い
//   - 最新が active (= deleteMark != 1) なら ErrForeignKeyViolation を返す
func verifyChildRecord(
	parentTrx *Transaction,
	indexRecord dictionary.IndexMetaRecord,
	candidateKey []byte,
) error {
	rowKey := lock.RowKey{MetaPageId: indexRecord.MetaPageId(), Key: candidateKey}
	if err := parentTrx.lockMgr.Lock(parentTrx.trxId, rowKey, lock.Shared); err != nil {
		return err
	}

	verifyMtr := parentTrx.NewReadMtr()
	defer verifyMtr.UnpinAll()

	tree := btree.NewTree(parentTrx.bufferPool, indexRecord.MetaPageId())
	latest, _, err := tree.FindByKey(verifyMtr, candidateKey)
	if errors.Is(err, btree.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if latest.Header()[0] != 1 {
		return ErrForeignKeyViolation
	}
	return nil
}
