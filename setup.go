package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	_ "modernc.org/sqlite"
)

func InitializeDatabase(dbPath string) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s", dbPath))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	createTables(db)
	importActors(db, "actors.csv")
	importDirectors(db, "directors.csv")
	importDirectorsGenres(db, "directors_genres.csv")
	importMovies(db, "movies.csv")
	importMoviesGenre(db, "movies_genre.csv")
	importRoles(db, "roles.csv")
}

func createTables(db *sql.DB) {
	tables := []string{
		`CREATE TABLE IF NOT EXISTS Actors (
            ActorID INTEGER PRIMARY KEY AUTOINCREMENT,
            FirstName TEXT NOT NULL,
            LastName TEXT NOT NULL,
            Gender TEXT
        );`,
		`CREATE TABLE IF NOT EXISTS Directors (
            DirectorID INTEGER PRIMARY KEY AUTOINCREMENT,
            FirstName TEXT NOT NULL,
            LastName TEXT NOT NULL
        );`,
		`CREATE TABLE IF NOT EXISTS DirectorsGenres (
            DirectorID INTEGER,
            Genre TEXT,
            Probability REAL,
            PRIMARY KEY (DirectorID, Genre),
            FOREIGN KEY (DirectorID) REFERENCES Directors(DirectorID)
        );`,
		`CREATE TABLE IF NOT EXISTS Movies (
            MovieID INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL,
            Year INTEGER,
            Rank REAL
        );`,
		`CREATE TABLE IF NOT EXISTS MoviesGenre (
            MovieID INTEGER,
            Genre TEXT,
            PRIMARY KEY (MovieID, Genre),
            FOREIGN KEY (MovieID) REFERENCES Movies(MovieID)
        );`,
		`CREATE TABLE IF NOT EXISTS Roles (
            ActorID INTEGER,
            MovieID INTEGER,
            Role TEXT,
            PRIMARY KEY (ActorID, MovieID, Role),
            FOREIGN KEY (ActorID) REFERENCES Actors(ActorID),
            FOREIGN KEY (MovieID) REFERENCES Movies(MovieID)
        );`,
	}

	for _, table := range tables {
		_, err := db.Exec(table)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func importCSV(db *sql.DB, csvFile string, insertSQL string) {
	file, err := os.Open(csvFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	for _, record := range records[1:] {
		args := make([]interface{}, len(record))
		for i, v := range record {
			args[i] = v
		}
		_, err := db.Exec(insertSQL, args...)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func importActors(db *sql.DB, csvFile string) {
	insertSQL := "INSERT INTO Actors (FirstName, LastName, Gender) VALUES (?, ?, ?)"
	importCSV(db, csvFile, insertSQL)
}

func importDirectors(db *sql.DB, csvFile string) {
	insertSQL := "INSERT INTO Directors (FirstName, LastName) VALUES (?, ?)"
	importCSV(db, csvFile, insertSQL)
}

func importDirectorsGenres(db *sql.DB, csvFile string) {
	insertSQL := "INSERT INTO DirectorsGenres (DirectorID, Genre, Probability) VALUES (?, ?, ?)"
	importCSV(db, csvFile, insertSQL)
}

func importMovies(db *sql.DB, csvFile string) {
	insertSQL := "INSERT INTO Movies (Name, Year, Rank) VALUES (?, ?, ?)"
	importCSV(db, csvFile, insertSQL)
}

func importMoviesGenre(db *sql.DB, csvFile string) {
	insertSQL := "INSERT INTO MoviesGenre (MovieID, Genre) VALUES (?, ?)"
	importCSV(db, csvFile, insertSQL)
}

func importRoles(db *sql.DB, csvFile string) {
	insertSQL := "INSERT INTO Roles (ActorID, MovieID, Role) VALUES (?, ?, ?)"
	importCSV(db, csvFile, insertSQL)
}
