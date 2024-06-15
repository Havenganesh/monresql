package monresql

import (
	"encoding/json"
	"fmt"
)

type dbResult struct {
	MongoDB    string
	Collection string
	Data       map[string]interface{}
}

type columnResult struct {
	Name string `db:"column_name"`
}

type tableColumn struct {
	Schema   string
	Table    string
	Column   string
	Type     string
	Message  string
	Solution string
}

func (t *tableColumn) uniqueIndex() string {
	return fmt.Sprintf("CREATE UNIQUE INDEX %s_service_uindex_on_%s ON %s.%s (%s);", t.Table, t.Column, t.Schema, t.Table, t.Column)
}

func (t *tableColumn) createColumn() string {
	return fmt.Sprintf(`ALTER TABLE %s.%s ADD %s %s NULL;`, t.Schema, t.Table, normalizeDotNotationToPostgresNaming(t.Column), t.Type)
}

// hasUniqueIndex
type hasUniqueIndex struct {
	Value int `db:"count"`
}

func (h *hasUniqueIndex) isValid() bool {
	return h.Value > 0
}

type mongoDB struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type postgresDB struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// nameQuoted is required for postgres table names
// and field names in case they conflict with SQL
// builtin functions
func (p postgresDB) nameQuoted() string {
	return fmt.Sprintf(`"%s"`, p.Name)
}

type field struct {
	Mongo    mongoDB    `json:"mongo"`
	Postgres postgresDB `json:"postgres"`
}
type (
	fields        map[string]field
	fieldsWrapper map[string]json.RawMessage
)

type coll struct {
	Name    string `json:"name"`
	PgTable string `json:"pg_table"`
	Fields  fields `json:"fields"`
}

func (c coll) pgTableQuoted() string {
	return fmt.Sprintf(`"%s"`, c.PgTable)
}

type collectionDelayed struct {
	Name    string          `json:"name"`
	PgTable string          `json:"pg_table"`
	Fields  json.RawMessage `json:"fields"`
}

type dBDelayed struct {
	Collections collectionsDelayed `json:"collections"`
}
type dB struct {
	Collections collections `json:"collections"`
}

type (
	collections        map[string]coll
	collectionsDelayed map[string]collectionDelayed
)

// Mapfile provides the core struct for
// the ultimate unmarshalled moresql.json
type fieldsMap map[string]dB

// ConfigDelayed provides lazy config loading
// to support shorthand and longhand variants
type configDelayed map[string]dBDelayed
