package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "modernc.org/sqlite"
)

func TestStartServer(t *testing.T) {
	dbPath := "moviedb.db"

	testQueries := []struct {
		query        string
		expectedCode int
		description  string
	}{
		{query: "SELECT * FROM Movies LIMIT 5", expectedCode: http.StatusOK, description: "return 5 movies"},
		{query: "SELECT COUNT(*) FROM Movies", expectedCode: http.StatusOK, description: "return number of movies in Table Movies"},
		{query: "SELECT * FROM Movies WHERE Rank != 'NULL' ORDER BY Rank DESC LIMIT 10", expectedCode: http.StatusOK, description: "return 10 highest rated movies"},
		{query: "SELECT Genre, COUNT(*) FROM MoviesGenre GROUP BY Genre", expectedCode: http.StatusOK, description: "return count of all genres"},
	}

	for _, tq := range testQueries {
		t.Run(tq.description, func(t *testing.T) {
			req, err := http.NewRequest("GET", fmt.Sprintf("/query?q=%s", tq.query), nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tq.expectedCode {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, tq.expectedCode)
			}

			responseBody, err := io.ReadAll(rr.Body)
			if err != nil {
				t.Fatalf("Failed to read response body: %v", err)
			}
			fmt.Printf("Query: %s\nResponse Body: %s\n", tq.query, responseBody)
		})
	}
}
