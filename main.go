package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"math/rand"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// open the database
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// apply pragmas to make the database faster
	db.Exec("PRAGMA journal_mode = WAL;")
	db.Exec("PRAGMA synchronous = NORMAL;")
	db.Exec("PRAGMA cache_size = -64000;")
	db.Exec("PRAGMA temp_store = MEMORY;")

	// create the table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS cash_flow_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		amount INTEGER NOT NULL, 
		date TIMESTAMP NOT NULL, 
		category TEXT NOT NULL,  -- income / expense
		necessity TEXT NOT NULL, -- need / want
		description TEXT, 		 -- optional
		user_id INTEGER NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		deleted_at TIMESTAMP
	)`)
	if err != nil {
		log.Fatalf("Error creating cash_flow_events table: %v", err)
	}

	// get events count
	var count int
	db.QueryRow("SELECT COUNT(*) FROM cash_flow_events").Scan(&count)
	if count < 1000 {
		// seed with fake events up to 1000
		for i := 0; i < 1000-count; i++ {
			db.Exec(`
			INSERT INTO cash_flow_events (amount, date, category, necessity, description, user_id) 
			VALUES (?, ?, ?, ?, ?, ?)`, rand.Intn(10000), time.Now().AddDate(0, 0, rand.Intn(30)), "income", "need", "description", rand.Intn(100))
		}
	}

	// log new events count
	db.QueryRow("SELECT COUNT(*) FROM cash_flow_events").Scan(&count)
	log.Printf("Events count: %d", count)

	// get a sample of 5 events
	rows, err := db.Query(`SELECT id, amount, category, user_id FROM cash_flow_events LIMIT 5`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	logTable(rows)

	prepareMVCentralStore(db)
	createMV(db, `SELECT id, amount, category, user_id FROM cash_flow_events LIMIT 5`, "cash_flow_events_mv")

	// remove the last 5 events
	db.Exec(`DELETE FROM cash_flow_events WHERE id IN (SELECT id FROM cash_flow_events ORDER BY id DESC LIMIT 5)`)

	// refresh the material view
	refreshMV(db, "cash_flow_events_mv")

	// delete db file
	os.Remove("./test.db")
}

// creates a table where we store material views => query connections
// so the user can easily refresh the material view in the future
func prepareMVCentralStore(db *sql.DB) error {
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

func createMV(db *sql.DB, query string, mvName string) error {
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
	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", mvName)
	for i, ct := range columnTypes {
		if i > 0 {
			createTableSQL += ", "
		}
		createTableSQL += fmt.Sprintf("%s %s", ct.Name(), sqliteType(ct.DatabaseTypeName()))
	}
	createTableSQL += ")"
	log.Println("creation query:", createTableSQL)

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
	_, err = tx.Exec(fmt.Sprintf("DELETE FROM %s", mvName))
	if err != nil {
		return fmt.Errorf("error deleting data: %w", err)
	}

	// Insert data into the new table
	insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM (%s)", mvName, query)
	_, err = tx.Exec(insertSQL)
	if err != nil {
		return fmt.Errorf("error inserting data: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	log.Println("insertion query:", insertSQL)

	// read the data from the new table
	rows, err = db.Query(fmt.Sprintf("SELECT * FROM %s", mvName))
	if err != nil {
		return fmt.Errorf("error reading data: %w", err)
	}
	defer rows.Close()
	logTable(rows)

	return nil
}

func refreshMV(db *sql.DB, mvName string) error {
	// find the query used to create the material view
	query := ""
	db.QueryRow("SELECT query FROM mv_central_store WHERE name = ?", mvName).Scan(&query)
	if query == "" {
		return fmt.Errorf("material view not found")
	}

	// re-create the material view
	return createMV(db, query, mvName)
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
