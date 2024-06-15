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
// the ultimate unmarshalled monresql.json
type fieldsMap map[string]dB

// ConfigDelayed provides lazy config loading
// to support shorthand and longhand variants
type configDelayed map[string]dBDelayed
