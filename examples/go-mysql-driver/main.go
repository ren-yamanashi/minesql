package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
)

func main() {
	password := os.Getenv("MINESQL_PASSWORD")
	if password == "" {
		log.Fatal("MINESQL_PASSWORD environment variable is required")
	}

	// TLS 設定 (自己署名証明書のためサーバー証明書の検証をスキップ)
	if err := mysql.RegisterTLSConfig("minesql", &tls.Config{
		InsecureSkipVerify: true,
	}); err != nil {
		log.Fatal(err)
	}

	dsn := fmt.Sprintf("root:%s@tcp(127.0.0.1:18888)/?tls=minesql", password)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 接続確認
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MineSQL")
	fmt.Println()

	// 1. CREATE TABLE
	runCreateTable(db)

	// 2. INSERT
	runInsert(db)

	// 3. SELECT
	runSelect(db)

	// 4. UPDATE
	runUpdate(db)

	// 5. DELETE
	runDelete(db)

	// 6. Transaction (COMMIT)
	runTransactionCommit(db)

	// 7. Transaction (ROLLBACK)
	runTransactionRollback(db)

	// 最終結果
	fmt.Println("=== Final Result ===")
	runSelect(db)
}

func runCreateTable(db *sql.DB) {
	fmt.Println("--- CREATE TABLE ---")
	_, err := db.Exec("CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id))")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created table: users")
	fmt.Println()
}

func runInsert(db *sql.DB) {
	fmt.Println("--- INSERT ---")
	result, err := db.Exec("INSERT INTO users (id, name) VALUES ('1', 'Alice'), ('2', 'Bob'), ('3', 'Charlie')")
	if err != nil {
		log.Fatal(err)
	}
	rows, _ := result.RowsAffected()
	fmt.Printf("Inserted %d rows\n", rows)
	fmt.Println()
}

func runSelect(db *sql.DB) {
	fmt.Println("--- SELECT * FROM users ---")
	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  id=%s, name=%s\n", id, name)
	}
	fmt.Println()
}

func runUpdate(db *sql.DB) {
	fmt.Println("--- UPDATE ---")
	result, err := db.Exec("UPDATE users SET name = 'Alice Updated' WHERE id = '1'")
	if err != nil {
		log.Fatal(err)
	}
	rows, _ := result.RowsAffected()
	fmt.Printf("Updated %d rows\n", rows)

	// 更新結果を確認
	row := db.QueryRow("SELECT name FROM users WHERE id = '1'")
	var name string
	if err := row.Scan(&name); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  id=1, name=%s\n", name)
	fmt.Println()
}

func runDelete(db *sql.DB) {
	fmt.Println("--- DELETE ---")
	result, err := db.Exec("DELETE FROM users WHERE id = '3'")
	if err != nil {
		log.Fatal(err)
	}
	rows, _ := result.RowsAffected()
	fmt.Printf("Deleted %d rows\n", rows)
	fmt.Println()
}

func runTransactionCommit(db *sql.DB) {
	fmt.Println("--- Transaction: COMMIT ---")

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := tx.Exec("INSERT INTO users (id, name) VALUES ('4', 'Dave')"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  INSERT id=4 (in transaction)")

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Committed")
	fmt.Println()
}

func runTransactionRollback(db *sql.DB) {
	fmt.Println("--- Transaction: ROLLBACK ---")

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := tx.Exec("DELETE FROM users WHERE id = '1'"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  DELETE id=1 (in transaction)")

	if err := tx.Rollback(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Rolled back - id=1 should still exist")
	fmt.Println()
}
