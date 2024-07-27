package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type FakeMV struct{}

// creates a table where we store material views => query connections
// so the user can easily refresh the material view in the future
func (f *FakeMV) Init(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS mv_central_store (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,  -- name of the material view
		query TEXT NOT NULL, -- query used to create the material view
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		deleted_at TIMESTAMP
	)`)
	if err != nil {
		return fmt.Errorf("error creating mv_central_store table: %w", err)
	}
	return nil
}

func (f *FakeMV) CreateMV(db *sql.DB, query string, mvName string) error {
	// Get data based on provided query
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	// Get column names and types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("error getting column types: %w", err)
	}

	// Create table schema
	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS [%s] (", mvName)
	for i, ct := range columnTypes {
		if i > 0 {
			createTableSQL += ", "
		}
		createTableSQL += fmt.Sprintf("%s %s", ct.Name(), sqliteType(ct.DatabaseTypeName()))
	}
	createTableSQL += ")"

	// Create the table
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback() // Rollback in case of error

	// insert the material view into the central store
	_, err = tx.Exec(`INSERT INTO mv_central_store (name, query) VALUES (?, ?)`, mvName, query)
	if err != nil {
		return fmt.Errorf("error inserting material view into central store: %w", err)
	}

	// Drop data of table
	_, err = tx.Exec(fmt.Sprintf("DELETE FROM [%s]", mvName))
	if err != nil {
		return fmt.Errorf("error deleting data: %w", err)
	}

	// Insert data into the new table
	insertSQL := fmt.Sprintf("INSERT INTO [%s] SELECT * FROM (%s)", mvName, query)
	_, err = tx.Exec(insertSQL)
	if err != nil {
		return fmt.Errorf("error inserting data: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

func (f *FakeMV) RefreshMV(db *sql.DB, mvName string) error {
	// find the query used to create the material view
	query := ""
	db.QueryRow("SELECT query FROM mv_central_store WHERE name = ?", mvName).Scan(&query)
	if query == "" {
		return fmt.Errorf("material view not found")
	}

	// re-create the material view
	return f.CreateMV(db, query, mvName)
}

func sqliteType(dbType string) string {
	switch dbType {
	case "INTEGER":
		return "INTEGER"
	case "REAL":
		return "REAL"
	case "TEXT":
		return "TEXT"
	case "BLOB":
		return "BLOB"
	default:
		return "TEXT"
	}
}
