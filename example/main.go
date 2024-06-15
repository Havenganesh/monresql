package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jmoiron/sqlx"

	"github.com/Havenganesh/monresql"
	_ "github.com/lib/pq"
)

func main() {
	dMap, err := monresql.LoadFieldsMap(jsonF)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	pq := getPostgresConn("postgres://postgres:Success97@localhost:5432/monresql")
	if pq == nil {
		os.Exit(1)
	}
	_, err = monresql.ValidateOrCreatePostgresTable(dMap, pq)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Validation Success, Table looks good")
	}

	defer pq.Close()
}

func getPostgresConn(url string) *sqlx.DB {
	db, err := sqlx.Connect("postgres", url)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// Ping the database to verify the connection.
	err = db.Ping()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	return db
}

var jsonF string = `{
	"monresql": {
	  "collections": {
		"students": {
		  "name": "students",
		  "pg_table": "students",
		  "fields": {
			"_id": "TEXT",
			"name": "TEXT",
			"age": "INTEGER",
			"rollNumber":{
			"Postgres": {"Name": "rollnumber","Type": "INTEGER"},
			"Mongo": {"Name": "rollNumber","Type": "INTEGER"}
			},
			"dob" : "TIMESTAMP",
			"subjects" : "TEXT[]",
			"class" : "TEXT",
			"markList": {
			  "Postgres": {"Name": "marklist","Type": "JSONB"},
			  "Mongo": {"Name": "markList","Type": "object"}
			}
		  }
		}
	  }
	}
  }`
