package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

func logTable(rows *sql.Rows) {
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	log.Printf("\n\n")

	// Print header
	for _, col := range columns {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print separator
	fmt.Println(strings.Repeat("-", 15*len(columns)))

	// Print rows
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Fatal(err)
		}

		for _, val := range values {
			fmt.Printf("%-15s", string(val))
		}
		fmt.Println()
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
	log.Printf("\n\n")
}
