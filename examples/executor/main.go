package main

import (
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"os"
)

func main() {
	dbPath := "executor/sample.db"
	os.Remove(dbPath) // 既存のファイルがあれば削除
	dm, err := disk.NewDiskManager(dbPath)
	if err != nil {
		panic(err)
	}
	bpm := bufferpool.NewBufferPoolManager(dm, 100)
	tbl := createTable(bpm)

	fullTableScan(bpm, tbl)
	rangeTableScan(bpm, tbl)
	searchConstPrimary(bpm, tbl)
	filterScan(bpm, tbl)
	fullIndexScanByFirstName(bpm, tbl)
	fullIndexScanByLastName(bpm, tbl)
	rangeIndexScan(bpm, tbl)
	searchConstUniqueIndex(bpm, tbl)
}
