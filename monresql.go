/*
 * Copyright (c) [2024] [ganesh v]
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
	Monresql: MongoDB to PostgreSQL Data Pipeline Tool

Overview
Monresql is a specialized library designed to facilitate efficient data replication, transfer, and synchronization from MongoDB to PostgreSQL databases. Inspired by similar tools like Moresql, Monresql focuses on unidirectional data movement, ensuring seamless integration and synchronization between MongoDB documents and PostgreSQL tables.

Key Features
Data Replication: Efficiently replicate data from MongoDB collections to corresponding PostgreSQL tables.

Incremental Updates: Support for incremental updates to keep PostgreSQL data up-to-date without full data reloads.

Performance Optimization: Optimizes data transfer processes for minimal latency and optimal resource utilization.

API Reference
LoadFieldsMap()
Loads a mapping file to define how MongoDB documents should be mapped to PostgreSQL tables.

ValidateOrCreatePostgresTable()
Validates the existence of a PostgreSQL table to ensure it's ready for data replication.

Replicate()
Initiates the data replication process from MongoDB to PostgreSQL based on the loaded mapping.

Sync()
Starts the synchronization process, ensuring that changes in MongoDB are reflected in PostgreSQL in real-time and also save the marker to sync from the last stopped mark if the service stopped

NewSyncOptions()
NewSyncOptions will return the pointer of the syncoptions struct with default values of

&syncOptions{checkpoint: true, checkPointPeriod: time.Minute * 1, lastEpoch: 0, reportPeriod: time.Minute * 1} then you can edit and change the values by set methods
*/
package monresql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

type syncStop func()

// LoadFieldsMap receive the file as json string and return FieldsMap
// please refer the moresql config file structure
func LoadFieldsMap(jsonString string) (fieldsMap, error) {
	config, err := jsonToFieldsMap(jsonString)
	if err != nil {
		fmt.Printf("Error While Validation : %s", err)
		return config, err
	}
	return config, nil
}

// ValidateOrCreatePostgresTable validates the postgres table fileds against the jsonfile, if table not
// exists and it is a valid file it creates postgres table,
// but you must create the Database yourself, always use the small letters in the postgres fields name
// please refer the complex structure
func ValidateOrCreatePostgresTable(fieldMap fieldsMap, pg *sqlx.DB) (string, error) {
	cmd := commands{}
	rsult := cmd.ValidateTablesAndColumns(fieldMap, pg)
	query := strings.Join(rsult, "")
	if len(rsult) > 0 {
		_, err := pg.DB.Exec(query)
		if err != nil {
			fmt.Println("Table Creation Error ", err)
			if strings.Contains(err.Error(), "already exists") {
				return query, errors.New("all Postgres fields must be in lower case \n to resolve that use complex structure\n" + COMPLEX)
			}
			return query, errors.New(err.Error())
		} else {
			fmt.Println("Table Creation Done.")
			rsult := cmd.ValidateTablesAndColumns(fieldMap, pg)
			if len(rsult) > 0 {
				query := strings.Join(rsult, "")
				return query, errors.New("all Postgres fields must be in lower case \n to resolve that use complex structure\n" + COMPLEX)
			}
		}
	}
	fmt.Println("Table Validation Success.")
	return "", nil
}

const COMPLEX string = `"fieldName": {
		"Postgres": {"Name": "field_name","Type": "JSONB"},
		"Mongo": {"Name": "fieldName","Type": "object"}
	      }`

// if the table is validated you can start the replication using this method
// please find sample  code in the example
func Replicate(config fieldsMap, pg *sqlx.DB, mongo *mongo.Client, replicaName string) string {
	var wg1 sync.WaitGroup
	sync1 := newReplicater(config, pg, mongo, replicaName)
	t := time.Now()
	wg1.Add(2)
	log.Println("Starting writer : " + replicaName)
	go sync1.Write(&wg1)
	log.Println("Starting reader : " + replicaName)
	go sync1.Read(&wg1)
	wg1.Wait()
	log.Info("===============================Full Sync Completed For : ", replicaName, " Duration : ", time.Since(t))
	defer pg.Close()
	defer mongo.Disconnect(context.Background())
	return "Replication Completed"
}

// if the table is validated you can start the sync using this method
// and it return a stop function, by using that method you can stop sync anytime
// please find sample  code in the example
func Sync(fieldMap fieldsMap, pg *sqlx.DB, client *mongo.Client, syncName string, syncOption *syncOptions) syncStop {
	if syncOption == nil {
		panic("syncOption not nil")
	}
	service := newsyncronizer(fieldMap, pg, client, syncName, syncOption)
	go service.serve()
	return service.stop
}
