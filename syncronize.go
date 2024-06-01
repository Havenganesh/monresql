package monresql

import (
	"context"
	"database/sql"
	"expvar"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/paulbellamy/ratecounter"
	"github.com/rwynn/gtm/v2"
	"github.com/serialx/hashring"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Tailer is the core struct for performing
// Mongo->Pg streaming.
type syncronizer struct {
	syncName     string
	pg           *sqlx.DB
	mgoClient    *mongo.Client
	counters     counters
	stopC        chan bool
	fan          map[string]gtm.OpChan
	checkpoint   *cmap.ConcurrentMap
	retryCount   int
	psqluserName string
	fieldMap     fieldsMap
	setting      settings
}
type settings struct {
	replayDuration time.Duration
	replaySecond   int64
	checkpoint     bool
}

var (
	runService       map[string]*syncronizer
	ctxCancelFuncMap map[string]context.CancelFunc
)

// Serve is the func necessary to start action
// when using Suture library
func (t *syncronizer) serve() {
	ctx, cancel := context.WithCancel(context.Background())
	ctxCancelFuncMap[t.syncName] = cancel
	t.write(ctx)
	t.read(ctx)
	t.report(ctx)
	t.checkpoints(ctx)

	<-t.stopC
}

// Stop is the func necessary to terminate action
// when using Suture library
func (t *syncronizer) stop() {
	cancel := ctxCancelFuncMap[t.syncName]
	cancel()
	t.pg.Close()
	t.mgoClient.Disconnect(context.Background())
	t.stopC <- true
}

func (t *syncronizer) startOverflowConsumers(c <-chan *gtm.Op, ctx context.Context) {
	for i := 1; i <= workerCountOverflow; i++ {
		go t.consumer(c, nil, ctx)
	}
}

func (t *syncronizer) opFilter(op *gtm.Op) bool {
	val := op.Data["_id"]
	log.Println("op values : ", op.Operation, op.Namespace)
	if val != nil {
		log.Println("this is check for validation : ", val)
		valkind := reflect.ValueOf(val).Kind().String()
		if valkind == "string" {
			no, err := strconv.Atoi(val.(string))
			if err != nil {
				log.Println("this is the error : ", err)
			} else if no%2 == 0 {
				return true
			}
		}
	}
	return false
}

func (t *syncronizer) newFan() map[string]gtm.OpChan {
	fan := make(map[string]gtm.OpChan)
	// Register Channels
	for dbName, db := range t.fieldMap {
		for collectionName := range db.Collections {
			fan[createFanKey(dbName, collectionName)] = make(gtm.OpChan, 1000)
		}
	}
	return fan
}

func (t *syncronizer) newOptions(timestamp epochTimestamp, replayDuration time.Duration) (*gtm.Options, error) {
	options := gtm.DefaultOptions()
	// options.Filter = t.OpFilter
	after := buildOptionAfterFromTimestamp(timestamp, replayDuration)
	tss, err := after(nil, nil)
	if err != nil {
		return nil, err
	}
	epoch, _ := gtm.ParseTimestamp(tss)
	log.Infof("Starting from epoch: %+v", epoch)
	options.After = after
	options.BufferSize = 500
	options.BufferDuration = time.Duration(500 * time.Millisecond)
	options.Ordering = gtm.Document
	return options, nil
}

func (t *syncronizer) startDedicatedConsumers(fan map[string]gtm.OpChan, overflow gtm.OpChan, ctx context.Context) {
	// Reserved workers for individual channels
	for k, c := range fan {
		workerPool := make(map[string]gtm.OpChan)
		var workers [workerCount]int
		for i := range workers {
			o := make(gtm.OpChan)
			workerPool[strconv.Itoa(i)] = o
		}
		keys := []string{}
		for k := range workerPool {
			keys = append(keys, k)
		}
		ring := hashring.New(keys)
		// wg.Add(1)
		go consistentBroker(c, ring, workerPool, ctx)
		for _, workerChan := range workerPool {
			go t.consumer(workerChan, overflow, ctx)
		}
		log.WithFields(log.Fields{
			"count":      workerCount,
			"collection": k,
		}).Debug("Starting worker(s)")
	}
}

type epochTimestamp int64

func buildOptionAfterFromTimestamp(timestamp epochTimestamp, replayDuration time.Duration) func(*mongo.Client, *gtm.Options) (primitive.Timestamp, error) {
	if timestamp != epochTimestamp(0) && int64(timestamp) < time.Now().Unix() {
		// We have a starting oplog entry
		f := func() time.Time { return time.Unix(int64(timestamp), 0) }
		return opTimestampWrapper(f, time.Duration(0))
	} else if replayDuration != time.Duration(0) {
		return opTimestampWrapper(time.Now, replayDuration)
	} else {
		return opTimestampWrapper(time.Now, time.Duration(0))
	}
}

func consistentBroker(in gtm.OpChan, ring *hashring.HashRing, workerPool map[string]gtm.OpChan, ctx context.Context) {
	for {
		select {
		case op := <-in:
			node, ok := ring.GetNode(fmt.Sprintf("%s", op.Id))
			if !ok {
				log.Error("Failed at getting worker node from hashring")
			} else {
				out := workerPool[node]
				out <- op
			}
		case <-ctx.Done():
			return
		}
	}
}

func (t *syncronizer) read(ctx context.Context) {
	metadata := fetchMetadata(t.pg, t.syncName, t.psqluserName)

	var lastEpoch int64
	if t.setting.replaySecond != 0 {
		lastEpoch = int64(t.setting.replaySecond)
	} else {
		lastEpoch = metadata.LastEpoch
	}
	options, err := t.newOptions(epochTimestamp(metadata.LastEpoch), t.setting.replayDuration)
	if err != nil {
		log.Println(err.Error())
	}
	ops, errs := gtm.Tail(t.mgoClient, options)
	g := gtmTail{ops, errs}
	// log.Info("Tailing mongo oplog")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-g.errs:
				if strings.Contains("i/o timeout", err.Error()) {
					// Restart gtm.Tail
					// Close existing channels to not leak resources
					log.Errorf("Problem connecting to mongo initiating reconnection: %s", err.Error())
					close(g.ops)
					close(g.errs)
					latest, ok := t.checkpoint.Get(t.syncName)
					if ok && latest != nil {
						metadata = latest.(monresqlMetadata)
						lastEpoch = metadata.LastEpoch
						options, err := t.newOptions(epochTimestamp(lastEpoch), t.setting.replayDuration)
						if err != nil {
							log.Println(err.Error())
						}
						ops, errs = gtm.Tail(t.mgoClient, options)
						g = gtmTail{ops, errs}
					} else {
						log.Printf("Exiting: Unable to recover from %s", err.Error())
					}
				} else {
					log.Printf("Exiting: Mongo tailer returned error %s", err.Error())
					if t.retryCount == 0 {
						Stop(t.syncName)
					} else {
						t.retryCount--
					}
				}
			case op := <-g.ops:
				t.counters.read.Incr(1)
				log.WithFields(log.Fields{
					"operation":  op.Operation,
					"collection": op.GetCollection(),
					"id":         op.Id,
				}).Debug("Received operation")
				// Check if we're watching for the collection
				db := op.GetDatabase()
				coll := op.GetCollection()
				key := createFanKey(db, coll)
				if c := t.fan[key]; c != nil {
					collection := t.fieldMap[db].Collections[coll]
					o := statement{collection}
					c <- ensureOpHasAllFields(op, o.mongoFields())
				} else {
					t.counters.skipped.Incr(1)
					log.Debug("Missing channel for this collection")
				}
				for k, v := range t.fan {
					if len(v) > 0 {
						log.Debugf("Channel %s has %d", k, len(v))
					}
				}
			}
		}
	}()
}

func (t *syncronizer) write(ctx context.Context) {
	t.fan = t.newFan()
	log.WithField("struct", t.fan).Debug("Fan")
	overflow := make(gtm.OpChan)
	t.startDedicatedConsumers(t.fan, overflow, ctx)
	t.startOverflowConsumers(overflow, ctx)
}

func (t *syncronizer) report(ctx context.Context) {
	reportPoint := viper.GetInt64("reportPointPeriod")
	reportPointFrequency := time.Duration(reportPoint) * time.Second
	// log.Println("report point period : ", reportPointFrequency)
	tiker := time.NewTicker(reportPointFrequency)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-tiker.C:
				t.ReportCounters()
			}
		}
	}()
}

func (t *syncronizer) saveCheckpoint(m monresqlMetadata) error {
	// log.Println("save check point called")
	q := queries{}
	result, err := t.pg.NamedExec(q.SaveMetadata(), m)
	if err != nil {
		log.Errorf("Unable to save into moresql_metadata: %+v, %+v", result, err.Error())
	}
	return err
}

func (t *syncronizer) checkpoints(ctx context.Context) {
	go func() {
		checkPointPeriod := viper.GetInt64("checkPointPeriod")
		checkpointFrequency := time.Duration(checkPointPeriod) * time.Second
		// log.Println("this is checkpoint frequency ", checkpointFrequency, "for this tailname : ", t.tailName)
		timer := time.NewTicker(checkpointFrequency)
		var epoch int64 = 0
		for {
			select {
			case <-timer.C:
				latest, ok := t.checkpoint.Get(t.syncName)
				if ok && latest != nil {
					data := latest.(monresqlMetadata)
					if epoch != data.LastEpoch {
						t.saveCheckpoint(data)
						log.Printf("Checkpoint Saved : \"%s\" epoch : %d", data.AppName, data.LastEpoch)
					}
					epoch = data.LastEpoch
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (t *syncronizer) consumer(in <-chan *gtm.Op, overflow chan<- *gtm.Op, ctx context.Context) {
	for {
		if overflow != nil && len(in) > workerCount {
			// Siphon off overflow
			select {
			case op := <-in:
				overflow <- op
			case <-ctx.Done():
				return
			}
			continue
		}
		select {
		case op := <-in:
			t.processOp(op)
			if t.setting.checkpoint {
				t.checkpoint.Set(t.syncName, t.opToMoresqlMetadata(op))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (t *syncronizer) opToMoresqlMetadata(op *gtm.Op) monresqlMetadata {
	ts, _ := gtm.ParseTimestamp(op.Timestamp)
	return monresqlMetadata{AppName: t.syncName, ProcessedAt: time.Now(), LastEpoch: int64(ts)}
}

func (t *syncronizer) getMongoDocById(id interface{}) map[string]interface{} {
	var result map[string]interface{}
	for dbName, v := range t.fieldMap {
		db := t.mgoClient.Database(dbName)
		for name := range v.Collections {
			coll := db.Collection(name)
			ctx := context.Background()
			defer ctx.Done()
			mongoResult := coll.FindOne(ctx, bson.M{"_id": id})
			if mongoResult != nil {
				mongoResult.Decode(&result)
				return result
			}

		}
	}
	return nil
}

func (t *syncronizer) processOp(op *gtm.Op) {
	collectionName := op.GetCollection()
	db := op.GetDatabase()
	st := replica{Config: t.fieldMap}
	o, c := st.statementFromDbCollection(db, collectionName)
	if op.IsUpdate() {
		op.Data = t.getMongoDocById(op.Id)
	}
	data := sanitizeData(c.Fields, op)

	switch {
	case op.IsInsert():
		t.counters.insert.Incr(1)
		upsertSQL := o.BuildUpsert()
		_, err := t.pg.NamedExec(upsertSQL, data)
		if err != nil {
			log.Error(upsertSQL, "data : ", data, " tailing insert error : ", err)
		}

	case op.IsUpdate():
		t.counters.update.Incr(1)
		updateSQL := o.BuildUpsert()
		_, err := t.pg.NamedExec(updateSQL, data)
		if err != nil {
			log.Error(updateSQL, "data : ", data, " tailing update error : ", err)
		}

	case op.IsDelete():
		t.counters.delete.Incr(1)
		deleteSQL := o.BuildDelete()
		_, err := t.pg.NamedExec(deleteSQL, data)
		if err != nil {
			log.Error(deleteSQL, "data : ", data, " tailing delete error : ", err)
		}
	}
}

func (t *syncronizer) ReportCounters() {
	for i, counter := range t.counters.All() {
		if counter.Rate() > 0 {
			log.Infof("%s : Tail \t%s\t per min: %d", t.syncName, i, counter.Rate())
		}
	}
}

func (t *syncronizer) msLag(epoch int32, nowFunc func() time.Time) int64 {
	// TODO: use time.Duration instead of this malarky
	ts := time.Unix(int64(epoch), 0)
	d := nowFunc().Sub(ts)
	nanoToMillisecond := func(t time.Duration) int64 { return t.Nanoseconds() / 1e6 }
	return nanoToMillisecond(d)
}

type monresqlMetadata struct {
	AppName     string    `db:"app_name"`
	LastEpoch   int64     `db:"last_epoch"`
	ProcessedAt time.Time `db:"processed_at"`
}

func newsyncronizer(fieldMap fieldsMap, pg *sqlx.DB, client *mongo.Client, syncName string) *syncronizer {
	checkpoint := cmap.New()
	return &syncronizer{
		fieldMap:     fieldMap,
		pg:           pg,
		mgoClient:    client,
		stopC:        make(chan bool, 5),
		counters:     buildCounters(syncName),
		checkpoint:   &checkpoint,
		syncName:     syncName,
		retryCount:   viper.GetInt("retryCount"),
		psqluserName: ""}
}

func fetchMetadata(pg *sqlx.DB, appName, username string) monresqlMetadata {
	metadata := monresqlMetadata{}
	q := queries{}
	err := pg.Get(&metadata, q.GetMetadata(), appName)
	// No rows means this is first time with table
	if err != nil && err != sql.ErrNoRows {
		log.Printf("Error while reading moresql_metadata table %+v", err)
		c := commands{}
		query := c.CreateTableSQL()
		query1 := strings.Replace(query, "$USERNAME", username, 1)
		log.Println("Executed Query : ", query)
		_, err := pg.DB.Exec(query1)
		if err != nil {
			log.Println("MetaTable Crreating Error : ", err)
		} else {
			log.Println("table created Sucessfuly")
		}
	}
	return metadata
}

type gtmTail struct {
	ops  gtm.OpChan
	errs chan error
}

type counters struct {
	insert  *ratecounter.RateCounter
	update  *ratecounter.RateCounter
	delete  *ratecounter.RateCounter
	read    *ratecounter.RateCounter
	skipped *ratecounter.RateCounter
}

func (c *counters) All() map[string]*ratecounter.RateCounter {
	cx := make(map[string]*ratecounter.RateCounter)
	cx["insert"] = c.insert
	cx["update"] = c.update
	cx["delete"] = c.delete
	cx["read"] = c.read
	cx["skipped"] = c.skipped
	return cx
}

func buildCounters(syncName string) (c counters) {
	c = counters{
		ratecounter.NewRateCounter(1 * time.Second),
		ratecounter.NewRateCounter(1 * time.Minute),
		ratecounter.NewRateCounter(1 * time.Minute),
		ratecounter.NewRateCounter(1 * time.Minute),
		ratecounter.NewRateCounter(1 * time.Minute),
	}
	time := fmt.Sprint(time.Now())
	insert := "insert/min" + syncName + time
	update := "update/min" + syncName + time
	delete := "delete/min" + syncName + time
	ops := "ops/min" + syncName + time
	skipped := "skipped/min" + syncName + time
	expvar.Publish(insert, c.insert)
	expvar.Publish(update, c.update)
	expvar.Publish(delete, c.delete)
	expvar.Publish(ops, c.read)
	expvar.Publish(skipped, c.skipped)
	return
}

func opTimestampWrapper(f func() time.Time, ago time.Duration) func(*mongo.Client, *gtm.Options) (primitive.Timestamp, error) {
	return func(*mongo.Client, *gtm.Options) (primitive.Timestamp, error) {
		now := f()
		inPast := now.Add(-ago)
		var c uint32 = 1
		ts, err := newPrimitiveTimeStamp(inPast, c) // NewMongoTimestamp(inPast, c)
		if err != nil {
			log.Error(err)
		}
		return ts, err
	}
}
