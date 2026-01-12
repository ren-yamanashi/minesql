package main

import "minesql/internal/executor"

func printRecords(exec executor.Executor) {
	records, err := executor.ExecutePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		println(string(record[0]), string(record[1]), string(record[2]))
	}
}
