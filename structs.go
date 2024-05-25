package monresql

import (
	"encoding/json"
	"fmt"
)

type DBResult struct {
	MongoDB    string
	Collection string
	Data       map[string]interface{}
}

type MongoResult struct {
	DB struct {
		Source      string
		Destination string
	}
	Data map[string]interface{}
}

type urls struct {
	mongo    string
	postgres string
}

type ColumnResult struct {
	Name string `db:"column_name"`
}

type TableColumn struct {
	Schema   string
	Table    string
	Column   string
	Type     string
	Message  string
	Solution string
}

func (t *TableColumn) uniqueIndex() string {
	return fmt.Sprintf("CREATE UNIQUE INDEX %s_service_uindex_on_%s ON %s.%s (%s);", t.Table, t.Column, t.Schema, t.Table, t.Column)
}

func (t *TableColumn) createColumn() string {
	return fmt.Sprintf(`ALTER TABLE %s.%s ADD %s %s NULL;`, t.Schema, t.Table, normalizeDotNotationToPostgresNaming(t.Column), t.Type)
}

// hasUniqueIndex
type hasUniqueIndex struct {
	Value int `db:"count"`
}

func (h *hasUniqueIndex) isValid() bool {
	return h.Value > 0
}

type Mongo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Postgres struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// nameQuoted is required for postgres table names
// and field names in case they conflict with SQL
// builtin functions
func (p Postgres) nameQuoted() string {
	return fmt.Sprintf(`"%s"`, p.Name)
}

type Field struct {
	Mongo    Mongo    `json:"mongo"`
	Postgres Postgres `json:"postgres"`
}
type (
	Fields         map[string]Field
	FieldShorthand map[string]string
	FieldsWrapper  map[string]json.RawMessage
)

type Collection struct {
	Name    string `json:"name"`
	PgTable string `json:"pg_table"`
	Fields  Fields `json:"fields"`
}

func (c Collection) pgTableQuoted() string {
	return fmt.Sprintf(`"%s"`, c.PgTable)
}

type CollectionDelayed struct {
	Name    string          `json:"name"`
	PgTable string          `json:"pg_table"`
	Fields  json.RawMessage `json:"fields"`
}

type DBDelayed struct {
	Collections CollectionsDelayed `json:"collections"`
}
type DB struct {
	Collections Collections `json:"collections"`
}

type (
	Collections        map[string]Collection
	CollectionsDelayed map[string]CollectionDelayed
)

// Mapfile provides the core struct for
// the ultimate unmarshalled moresql.json
type FieldsMap map[string]DB

// ConfigDelayed provides lazy config loading
// to support shorthand and longhand variants
type ConfigDelayed map[string]DBDelayed
