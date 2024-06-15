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

func Sync(fieldMap fieldsMap, pg *sqlx.DB, client *mongo.Client, syncName string, syncOption *syncOptions) syncStop {
	if syncOption == nil {
		panic("syncOption not nil")
	}
	service := newsyncronizer(fieldMap, pg, client, syncName, syncOption)
	go service.serve()
	return service.stop
}
