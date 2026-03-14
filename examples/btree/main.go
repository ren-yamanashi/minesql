package main

import "os"

func main() {
	dataDir := "examples/btree/data"

	// 既存のデータディレクトリがあれば削除
	os.RemoveAll(dataDir)
	os.MkdirAll(dataDir, 0750)

	insert(dataDir)
	scan()
	searchKey()
	update()
	delete()
}