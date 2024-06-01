package monresql

import (
	"context"
	"expvar"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/paulbellamy/ratecounter"
	"github.com/rwynn/gtm/v2"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const workerCountOverflow = 500
const workerCount = 5

type replica struct {
	Config      fieldsMap
	Output      *sqlx.DB
	Mongoclient *mongo.Client
	C           chan dbResult
	done        chan bool

	insertCounter *ratecounter.RateCounter
	readCounter   *ratecounter.RateCounter
}

func (z *replica) Read(wg1 *sync.WaitGroup) {
	for dbName, v := range z.Config {
		db := z.Mongoclient.Database(dbName)
		for name := range v.Collections {
			coll := db.Collection(name)
			ctx := context.Background()
			defer ctx.Done()
			cursor, err := coll.Find(ctx, bson.M{})
			if err != nil {
				log.Println("no cursor document found error on the find ", err)
			}
			var result map[string]interface{}
			for cursor.TryNext(ctx) {
				if err = bson.Unmarshal(cursor.Current, &result); err != nil {
					log.Println("unmarshal bson mongodb error : ", err)
				} else {
					z.readCounter.Incr(1)
					z.C <- dbResult{dbName, name, result}
					result = make(map[string]interface{})
				}
			}
		}
	}
	close(z.C)
	wg1.Done()
}

func (z *replica) Write(wg1 *sync.WaitGroup) {
	var workers [workerCountOverflow]int
	tables := z.buildTables()
	for range workers {
		wg1.Add(1)
		go z.writer(&tables, wg1)
	}
	wg1.Done()
}

func (z *replica) writer(tables *cmap.ConcurrentMap, wg1 *sync.WaitGroup) {
	for e := range z.C {
		key := createFanKey(e.MongoDB, e.Collection)
		v, ok := tables.Get(key)
		if ok && !v.(bool) {
			// Table doesn't exist, skip
			break
		}
		o, coll := z.statementFromDbCollection(e.MongoDB, e.Collection)
		op := buildOpFromMgo(o.mongoFields(), e, coll)
		s := o.BuildUpsert()
		_, err := z.Output.NamedExec(s, op.Data)
		z.insertCounter.Incr(1)
		if err != nil {
			log.WithFields(log.Fields{
				"description": err,
			}).Error("Error")
			if err.Error() == fmt.Sprintf(`pq: relation "%s" does not exist`, e.Collection) {
				tables.Set(key, false)
			}
		}
	}
	wg1.Done()
}

func (z *replica) statementFromDbCollection(db string, collectionName string) (statement, coll) {
	c := z.Config[db].Collections[collectionName]
	return statement{c}, c
}

func (z *replica) buildTables() (tables cmap.ConcurrentMap) {
	tables = cmap.New()
	for dbName, db := range z.Config {
		for collectionName := range db.Collections {
			// Assume all tables are present
			tables.Set(createFanKey(dbName, collectionName), true)
		}
	}
	return
}

func buildOpFromMgo(mongoFields []string, e dbResult, coll coll) *gtm.Op {
	var op gtm.Op
	op.Data = e.Data
	opRef := ensureOpHasAllFields(&op, mongoFields)
	opRef.Id = e.Data["_id"]
	// Set to I so we are consistent about these beings inserts
	// This avoids our guardclause in sanitize
	opRef.Operation = "i"
	data := sanitizeData(coll.Fields, opRef)
	opRef.Data = data
	return opRef
}

func newReplicater(config fieldsMap, pg *sqlx.DB, mongo *mongo.Client, replicaName string) replica {
	c := make(chan dbResult)
	insertCounter := ratecounter.NewRateCounter(1 * time.Second)
	readCounter := ratecounter.NewRateCounter(1 * time.Second)
	time := fmt.Sprint(time.Now())
	insert := "insert/sec " + replicaName + time
	read := "read/sec " + replicaName + time
	log.Info("inserted value :", insert, read, replicaName)
	expvar.Publish(insert, insertCounter)
	expvar.Publish(read, readCounter)
	done := make(chan bool, 2)
	sync := replica{config, pg, mongo, c, done, insertCounter, readCounter}
	return sync
}
