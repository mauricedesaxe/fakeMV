package main

import (
	"database/sql"
	"math/rand"
	"os"
	"testing"
	"time"
)

func setupDb() error {
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		return err
	}
	defer db.Close()

	// apply pragmas to make the database faster
	db.Exec("PRAGMA journal_mode = WAL;")
	db.Exec("PRAGMA synchronous = NORMAL;")
	db.Exec("PRAGMA cache_size = -64000;")
	db.Exec("PRAGMA temp_store = MEMORY;")
	return nil
}

func cleanup() {
	// delete db file
	os.Remove("./test.db")
}

func TestCashFlowEvents(t *testing.T) {
	err := setupDb()
	if err != nil {
		t.Errorf("Error setting up database: %v", err)
	}

	t.Run("Seed DB", func(t *testing.T) {
		db, err := sql.Open("sqlite3", "./test.db")
		if err != nil {
			t.Errorf("Error opening database: %v", err)
		}
		defer db.Close()

		// create the table
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS events (
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
			t.Errorf("Error creating events table: %v", err)
		}

		// get events count
		var count int
		db.QueryRow("SELECT COUNT(*) FROM events").Scan(&count)
		if count < 1000 {
			// seed with fake events up to 1000
			for i := 0; i < 1000-count; i++ {
				db.Exec(`
			INSERT INTO events (amount, date, category, necessity, description, user_id) 
			VALUES (?, ?, ?, ?, ?, ?)`, rand.Intn(10000), time.Now().AddDate(0, 0, rand.Intn(30)), "income", "need", "description", rand.Intn(100))
			}
		}

		// expect 1000 events
		db.QueryRow("SELECT COUNT(*) FROM events").Scan(&count)
		if count != 1000 {
			t.Errorf("Expected 1000 events, got %d", count)
		}
	})

	t.Run("Basic CreateMV test", func(t *testing.T) {
		fakeMV := &FakeMV{}

		// open database
		db, err := sql.Open("sqlite3", "./test.db")
		if err != nil {
			t.Errorf("Error opening database: %v", err)
		}
		defer db.Close()

		// get a rawSample of 5 events
		rawSample, err := db.Query(`SELECT id, amount, category, user_id FROM events ORDER BY id DESC LIMIT 5`)
		if err != nil {
			t.Errorf("Error getting data from raw table: %v", err)
		}
		defer rawSample.Close()

		err = fakeMV.Init(db)
		if err != nil {
			t.Errorf("Error preparing material view central store: %v", err)
		}
		err = fakeMV.CreateMV(db, `SELECT id, amount, category, user_id FROM events ORDER BY id DESC LIMIT 5`, "events_sample")
		if err != nil {
			t.Errorf("Error creating material view: %v", err)
		}

		// get data from the material view
		mvSample, err := db.Query(`SELECT * FROM events_sample`)
		if err != nil {
			t.Errorf("Error getting data from material view: %v", err)
		}
		defer mvSample.Close()

		// test if the firstMvSample is the same as the firstRawSample
		for rawSample.Next() && mvSample.Next() {
			var id1, id2 int
			var amount1, amount2 int
			var category1, category2 string
			var user_id1, user_id2 int
			err = rawSample.Scan(&id1, &amount1, &category1, &user_id1)
			if err != nil {
				t.Errorf("Error scanning raw sample: %v", err)
			}
			err = mvSample.Scan(&id2, &amount2, &category2, &user_id2)
			if err != nil {
				t.Errorf("Error scanning MV sample: %v", err)
			}
			if id1 != id2 || amount1 != amount2 || category1 != category2 || user_id1 != user_id2 {
				t.Errorf("Mismatch: Raw(%d, %d, %s, %d) != MV(%d, %d, %s, %d)",
					id1, amount1, category1, user_id1,
					id2, amount2, category2, user_id2)
			}
		}
	})

	t.Run("Basic RefreshMV test", func(t *testing.T) {
		fakeMV := &FakeMV{}

		// open database
		db, err := sql.Open("sqlite3", "./test.db")
		if err != nil {
			t.Errorf("Error opening database: %v", err)
		}
		defer db.Close()

		// remove the last 5 events & refresh the material view
		_, err = db.Exec(`DELETE FROM events WHERE id IN (SELECT id FROM events ORDER BY id DESC LIMIT 5)`)
		if err != nil {
			t.Errorf("Error deleting events: %v", err)
		}
		err = fakeMV.RefreshMV(db, "events_sample")
		if err != nil {
			t.Errorf("Error refreshing material view: %v", err)
		}

		// get data from raw table and from material view
		rawSample, err := db.Query(`SELECT id, amount, category, user_id FROM events ORDER BY id DESC LIMIT 5`)
		if err != nil {
			t.Errorf("Error getting data from raw table: %v", err)
		}
		mvSample, err := db.Query(`SELECT * FROM events_sample`)
		if err != nil {
			t.Errorf("Error getting data from material view: %v", err)
		}

		// test if the secondMvSample is the same as the secondRawSample
		for rawSample.Next() && mvSample.Next() {
			var id1, id2 int
			var amount1, amount2 int
			var category1, category2 string
			var user_id1, user_id2 int
			err = rawSample.Scan(&id1, &amount1, &category1, &user_id1)
			if err != nil {
				t.Errorf("Error scanning raw sample: %v", err)
			}
			err = mvSample.Scan(&id2, &amount2, &category2, &user_id2)
			if err != nil {
				t.Errorf("Error scanning MV sample: %v", err)
			}
			if id1 != id2 || amount1 != amount2 || category1 != category2 || user_id1 != user_id2 {
				t.Errorf("Mismatch: Raw(%d, %d, %s, %d) != MV(%d, %d, %s, %d)",
					id1, amount1, category1, user_id1,
					id2, amount2, category2, user_id2)
			}
		}
	})

	t.Run("Create MV for a user's last 30 events", func(t *testing.T) {
		fakeMV := &FakeMV{}

		// open database
		db, err := sql.Open("sqlite3", "./test.db")
		if err != nil {
			t.Errorf("Error opening database: %v", err)
		}
		defer db.Close()

		// get a rawSample of a user's last 30 events
		rawSample, err := db.Query(`SELECT id, amount, category, user_id FROM events WHERE user_id = 1 ORDER BY date DESC LIMIT 30`)
		if err != nil {
			t.Errorf("Error getting data from raw table: %v", err)
		}
		defer rawSample.Close()

		err = fakeMV.CreateMV(db, `
			SELECT id, amount, category, user_id FROM events WHERE user_id = 1 ORDER BY date DESC LIMIT 30`, "user_1_events_last_30")
		if err != nil {
			t.Errorf("Error creating material view: %v", err)
		}

		// get data from the material view
		mvSample, err := db.Query(`SELECT * FROM user_1_events_last_30`)
		if err != nil {
			t.Errorf("Error getting data from material view: %v", err)
		}
		defer mvSample.Close()

		// test if the firstMvSample is the same as the firstRawSample
		for rawSample.Next() && mvSample.Next() {
			var id1, id2 int
			var amount1, amount2 int
			var category1, category2 string
			var user_id1, user_id2 int
			err = rawSample.Scan(&id1, &amount1, &category1, &user_id1)
			if err != nil {
				t.Errorf("Error scanning raw sample: %v", err)
			}
			err = mvSample.Scan(&id2, &amount2, &category2, &user_id2)
			if err != nil {
				t.Errorf("Error scanning MV sample: %v", err)
			}
			if id1 != id2 || amount1 != amount2 || category1 != category2 || user_id1 != user_id2 {
				t.Errorf("Mismatch: Raw(%d, %d, %s, %d) != MV(%d, %d, %s, %d)",
					id1, amount1, category1, user_id1,
					id2, amount2, category2, user_id2)
			}
		}
	})

	cleanup()
}
