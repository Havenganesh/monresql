package monresql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

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

func ValidatePostgresTable(fieldMap fieldsMap, pg *sqlx.DB) string {
	cmd := commands{}
	rsult := cmd.ValidateTablesAndColumns(fieldMap, pg)
	result := ""
	for _, str := range rsult {
		result = result + str
	}
	return result
}

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

func Sync(fieldMap fieldsMap, pg *sqlx.DB, client *mongo.Client, syncName string) {
	service := newsyncronizer(fieldMap, pg, client, syncName)
	service.serve()
	runService[syncName] = service
}

func Stop(syncName string) {
	service := runService[syncName]
	service.stop()
	runService[syncName] = nil
	ctxCancelFuncMap[syncName] = nil
}
