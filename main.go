package main

import (
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

func main() {
	dbPath := "moviedb.db"
	if !fileExists(dbPath) {
		InitializeDatabase(dbPath)
	} else {
		fmt.Println("Database already exists. No need to fill it again.")
	}

	StartServer(dbPath)
}
