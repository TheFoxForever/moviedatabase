package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "modernc.org/sqlite"
)

func StartServer(dbPath string) {
	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if query == "" {
			http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
			return
		}

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to open database: %v", err), http.StatusInternalServerError)
			return
		}
		defer db.Close()

		rows, err := db.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to execute query: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get columns: %v", err), http.StatusInternalServerError)
			return
		}

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			columnsData := make([]interface{}, len(columns))
			columnsPointers := make([]interface{}, len(columns))
			for i := range columnsData {
				columnsPointers[i] = &columnsData[i]
			}

			if err := rows.Scan(columnsPointers...); err != nil {
				http.Error(w, fmt.Sprintf("Failed to scan row: %v", err), http.StatusInternalServerError)
				return
			}

			row := make(map[string]interface{})
			for i, colName := range columns {
				val := columnsPointers[i].(*interface{})
				row[colName] = *val
			}
			results = append(results, row)
		}

		if err := rows.Err(); err != nil {
			http.Error(w, fmt.Sprintf("Row error: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(results); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode results: %v", err), http.StatusInternalServerError)
		}
	})

	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
