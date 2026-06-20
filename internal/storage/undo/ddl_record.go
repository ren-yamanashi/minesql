package undo

// DDLRecordType は DDL Undo レコードの種別
type DDLRecordType int

const (
	ddlRecordTypeUnknown DDLRecordType = iota
	// DDLRecordTypeCreateBTree は B+Tree 作成に対する DDL Undo の種別
	DDLRecordTypeCreateBTree
	// DDLRecordTypeMetaInsert はメタテーブルへのレコード挿入に対する DDL Undo の種別
	DDLRecordTypeMetaInsert
	// DDLRecordTypeAllocateFileId は物理ファイル確保に対する DDL Undo の種別
	DDLRecordTypeAllocateFileId
)

func (t DDLRecordType) String() string {
	switch t {
	case DDLRecordTypeCreateBTree:
		return "CreateBTree"
	case DDLRecordTypeMetaInsert:
		return "MetaInsert"
	case DDLRecordTypeAllocateFileId:
		return "AllocateFileId"
	default:
		return "Unknown"
	}
}
