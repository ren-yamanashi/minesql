package main

import (
	"minesql/internal/engine"
	"os"
)

func main() {
	dataDir := "examples/executor/data"
	os.RemoveAll(dataDir) // 既存のデータディレクトリがあれば削除
	os.MkdirAll(dataDir, 0750)

	// StorageManager を初期化
	os.Setenv("MINESQL_DATA_DIR", dataDir)
	os.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Init()

	createTable()
	fullTableScan()
	rangeTableScan()
	searchConstPrimary()
	filterScan()
	fullIndexScanByFirstName()
	fullIndexScanByLastName()
	rangeIndexScan()
	searchConstUniqueIndex()
	updateRecords()
	deleteRecords()
}
