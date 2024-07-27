package main

import (
	"database/sql"
	"log"
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
			VALUES (?, ?, ?, ?, ?, ?)`, rand.Intn(10000), time.Now().AddDate(0, 0, rand.Intn(30)), "income", "need", "description", 1)
		}
	}
}
