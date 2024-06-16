# Monresql: MongoDB to PostgreSQL Data Pipeline Tool Library A Go Library for Building Data Pipelines from MongoDB (including MongoDB 6) to PostgreSQL

## Overview

Monresql is a specialized library designed to facilitate efficient data replication, transfer, and synchronization from MongoDB to PostgreSQL databases. Inspired by similar tools like Moresql, Monresql focuses on unidirectional data movement, ensuring seamless integration and synchronization between MongoDB documents and PostgreSQL tables.

## Description:

Monresql is a robust and efficient Go library designed to help developers build seamless data pipeline applications that transfer data from MongoDB
(including the latest MongoDB 6) to PostgreSQL. Inspired by the popular Moresql library,
Monresql offers a reliable solution for data migration and synchronization tasks, incorporating the latest MongoDB 6 features and capabilities.

## Key Features

- **Data Replication**: Efficiently replicate data from MongoDB collections to corresponding PostgreSQL tables.
- **Incremental Updates**: Support for incremental updates to keep PostgreSQL data up-to-date without full data reloads.
- **Performance Optimization**: Optimizes data transfer processes for minimal latency and optimal resource utilization.

## API Reference

### `LoadFieldsMap()`

Loads a mapping file to define how MongoDB documents should be mapped to PostgreSQL tables.

### `ValidateOrCreatePostgresTable()`

Validates the existence of a PostgreSQL table to ensure it's ready for data replication.

### `Replicate()`

Initiates the data replication process from MongoDB to PostgreSQL based on the loaded mapping.

### `Sync()`

Starts the synchronization process, ensuring that changes in MongoDB are reflected in PostgreSQL in real-time and also save the marker to sync from the last stopped mark if the service stopped

### `NewSyncOptions()`

NewSyncOptions will return the pointer of the syncoptions struct with default values of

&syncOptions{checkpoint: true, checkPointPeriod: time.Minute _ 1, lastEpoch: 0, reportPeriod: time.Minute _ 1}
then you can edit and change the values by set methods

## Getting Started

### Installation

To install Monresql, follow these steps:
if you like to add or edit the core logics of the codes

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/monresql.git

   ```

2. get as go library :
   ```bash
   go get github.com/Havenganesh/monresql
   ```

## Credits:

Monresql draws inspiration from the Moresql cli application, acknowledging its contribution to the field of data migration and integration.
The development of Monresql has been guided by the core principles and best practices established by Moresql,ensuring a reliable and efficient data pipeline solution.

## Example

please find the example code in the github repository

```bash
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
    	monresql.Replicate(dMap, pq, clint, "students")
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


```
