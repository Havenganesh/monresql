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
	"fmt"
	"sort"
	"strings"
)

// statement provides functions for building up upsert/insert/update/allowDeletes
// sql commands appropriate for a gtm.Op.Data
type statement struct {
	Collection coll
}

func (o *statement) prefixColon(s string) string {
	return fmt.Sprintf(":%s", s)
}

func (o *statement) mongoFields() []string {
	var fields []string
	for _, k := range o.sortedKeys() {
		v := o.Collection.Fields[k]
		fields = append(fields, v.Mongo.Name)
	}
	return fields
}

func (o *statement) postgresFields() []string {
	var fields []string
	for _, k := range o.sortedKeys() {
		v := o.Collection.Fields[k]
		fields = append(fields, v.Postgres.Name)
	}
	return fields
}

func (o *statement) postgresFieldsQuoted() []string {
	var fields []string
	for _, k := range o.sortedKeys() {
		v := o.Collection.Fields[k]
		fields = append(fields, v.Postgres.nameQuoted())
	}
	return fields
}

func (o *statement) colonFields() []string {
	var withColons []string
	for _, f := range o.postgresFields() {
		withColons = append(withColons, o.prefixColon(f))
	}
	return withColons
}

func (o *statement) joinedPlaceholders() string {
	return strings.Join(o.colonFields(), ", ")
}

func (o *statement) joinLines(sx ...string) string {
	return strings.Join(sx, "\n")
}

func (o *statement) buildAssignment() string {
	set := []string{}
	for _, k := range o.sortedKeys() {
		v := o.Collection.Fields[k]
		if k != "_id" {
			// Accesses data that has already been sanitized into postgres naming
			set = append(set, fmt.Sprintf(`%s = :%s`, v.Postgres.nameQuoted(), v.Postgres.Name))
		}
	}
	return strings.Join(set, ", ")
}

func (o *statement) buildUpdateAssignment(fields []string) string {
	set := []string{}
	for _, k := range fields {
		v := o.Collection.Fields[k]
		if k != "_id" {
			// Accesses data that has already been sanitized into postgres naming
			set = append(set, fmt.Sprintf(`%s = :%s`, v.Postgres.nameQuoted(), v.Postgres.Name))
		}
	}
	return strings.Join(set, ", ")
}

func (o *statement) sortedKeys() []string {
	var keys []string
	for k := range o.Collection.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (o *statement) id() field {
	return o.Collection.Fields["_id"]
}

func (o *statement) whereById() string {
	id := o.id()
	return fmt.Sprintf(`WHERE %s = :%s`, id.Postgres.nameQuoted(), id.Mongo.Name)
}

func (o *statement) BuildUpsert() string {
	insert := o.BuildInsert()
	onConflict := fmt.Sprintf("ON CONFLICT (%s)", o.id().Postgres.nameQuoted())
	doUpdate := fmt.Sprintf("DO UPDATE SET %s;", o.buildAssignment())
	output := o.joinLines(insert, onConflict, doUpdate)
	return output
}

func (o *statement) BuildInsert() string {
	insertInto := fmt.Sprintf("INSERT INTO %s (%s)", o.Collection.pgTableQuoted(), strings.Join(o.postgresFieldsQuoted(), ", "))
	values := fmt.Sprintf("VALUES (%s)", o.joinedPlaceholders())
	output := o.joinLines(insertInto, values)
	return output
}

func (o *statement) BuildUpdate(fields []string) string {
	update := fmt.Sprintf("UPDATE %s", o.Collection.pgTableQuoted())
	set := fmt.Sprintf("SET %s", o.buildUpdateAssignment(fields))
	where := fmt.Sprintf("%s;", o.whereById())
	return o.joinLines(update, set, where)
}

func (o *statement) BuildDelete() string {
	return fmt.Sprintf("DELETE FROM %s %s;", o.Collection.pgTableQuoted(), o.whereById())
}
