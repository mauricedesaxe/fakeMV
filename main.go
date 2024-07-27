package main

import (
	"database/sql"
	"log"

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
}
