package main

import (
	"minesql/internal/storage"
	"os"
)

func main() {
	dataDir := "examples/executor/data"
	os.RemoveAll(dataDir) // 既存のデータディレクトリがあれば削除
	os.MkdirAll(dataDir, 0755)

	// StorageEngine を初期化
	os.Setenv("MINESQL_DATA_DIR", dataDir)
	os.Setenv("MINESQL_BUFFER_SIZE", "100")
	storage.InitStorageEngine()

	createTable()
	fullTableScan()
	rangeTableScan()
	searchConstPrimary()
	filterScan()
	fullIndexScanByFirstName()
	fullIndexScanByLastName()
	rangeIndexScan()
	searchConstUniqueIndex()
}
