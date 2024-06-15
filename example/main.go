package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

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
	defer pq.Close()
	//validate the postges table
	_, err = monresql.ValidateOrCreatePostgresTable(dMap, pq)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Validation Success, Table looks good")
	}
	clint, err := getMongoConn("mongodb://localhost:27017")
	if err != nil {
		log.Fatal(err)
	}
	//replicate the data from mongo to postgresql
	// monresql.Replicate(dMap, pq, clint, "students")
	option := monresql.NewSyncOptions()
	option.SetCheckPointPeriod(time.Second * 5)
	stop := monresql.Sync(dMap, pq, clint, "students", option)
	fmt.Println("sync called after")
	time.Sleep(time.Minute * 1)
	fmt.Println("waited end")
	stop()
	time.Sleep(time.Minute * 10)

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

func getMongoConn(url string) (*mongo.Client, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(url))
	if err != nil {
		log.Println(err)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	} else {
		log.Println("mongo connected")
	}
	return client, nil
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
			"subjects" : "JSONB",
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
