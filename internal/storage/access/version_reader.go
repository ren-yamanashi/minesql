package access

// VersionReader は undo チェーンを辿って可視なバージョンのレコードを見つける
type VersionReader struct {
	undoMgr *UndoManager
}

func NewVersionReader(undoMgr *UndoManager) *VersionReader {
	return &VersionReader{undoMgr: undoMgr}
}

// RecordVersion は行の 1 つのバージョンを表す
type RecordVersion struct {
	LastModified TrxId    // この行を最後に INSERT/UPDATE したトランザクション ID
	RollPtr      UndoPtr  // undo ログレコードへのポインタ (旧バージョンへの参照)
	DeleteMark   byte     // 削除マーク (0: 有効, 1: 削除)
	Columns      [][]byte // レコードのカラムデータ (プライマリキー + 非キーカラム)
}

// ReadVisibleVersion は ReadView に基づいて可視なバージョンのレコードを返す
//
// 現在の行の lastModified が不可視の場合、rollPtr から undo チェーンを辿り、
// 可視なバージョンが見つかるまで遡る。見つからなければ found=false を返す。
func (vr *VersionReader) ReadVisibleVersion(rv *ReadView, current RecordVersion) (RecordVersion, bool, error) {
	// 現在のバージョンが可視ならそのまま返す
	if rv.IsVisible(current.LastModified) {
		return current, true, nil
	}

	// undo チェーンを辿って可視なバージョンを探す
	currentRollPtr := current.RollPtr
	for !currentRollPtr.IsNull() {
		raw, readErr := vr.undoMgr.ReadAt(currentRollPtr)
		if readErr != nil {
			return RecordVersion{}, false, readErr
		}

		f, deserializeErr := DeserializeUndoRecord(raw)
		if deserializeErr != nil {
			return RecordVersion{}, false, deserializeErr
		}

		// PrevLastModified=0 は前バージョンが存在しないことを意味する (TrxId は 1 から採番される)
		if f.PrevLastModified != 0 && rv.IsVisible(f.PrevLastModified) {
			return RecordVersion{
				LastModified: f.PrevLastModified,
				RollPtr:      f.PrevRollPtr,
				DeleteMark:   0,
				Columns:      f.ColumnSets[0],
			}, true, nil
		}

		currentRollPtr = f.PrevRollPtr
	}

	// チェーンの末尾まで辿っても可視なバージョンが見つからなかった
	return RecordVersion{}, false, nil
}
