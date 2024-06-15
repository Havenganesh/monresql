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
	"regexp"

	"strings"

	log "github.com/sirupsen/logrus"
)

func jsonToFieldsMap(s string) (fieldsMap, error) {
	config := fieldsMap{}
	var configDelayed configDelayed
	err := json.Unmarshal([]byte(s), &configDelayed)
	if err != nil {
		log.Println("LoadConfig String ", err)
		return config, err
	}
	for k, v := range configDelayed {
		db := dB{}
		collections := collections{}
		db.Collections = collections
		for k, v := range v.Collections {
			coll := coll{Name: v.Name, PgTable: v.PgTable}
			var fields1 fields
			fields1, err = jsonToFields(string(v.Fields))
			if err != nil {
				log.Warnf("JSON Config decoding error: %s", err)
				return nil, fmt.Errorf("unable to decode %w", err)
			}
			coll.Fields = fields1
			db.Collections[k] = coll
		}
		config[k] = db
	}
	return config, nil
}

func jsonToFields(s string) (fields, error) {
	var init fieldsWrapper
	var err error
	result := fields{}
	err = json.Unmarshal([]byte(s), &init)
	for k, v := range init {
		field1 := field{}
		str := ""
		if err := json.Unmarshal(v, &field1); err == nil {
			result[k] = field1
		} else if err := json.Unmarshal(v, &str); err == nil {
			// Convert shorthand to longhand Field
			f := field{
				mongoDB{k, str},
				postgresDB{normalizeDotNotationToPostgresNaming(k), mongoToPostgresTypeConversion(str)},
			}
			result[k] = f
		} else {
			errLong := json.Unmarshal(v, &field1)
			errShort := json.Unmarshal(v, &str)
			err = fmt.Errorf("could not decode field. long decoding %+v. short decoding %+v", errLong, errShort)
			return nil, err
		}
	}
	return result, err
}

func mongoToPostgresTypeConversion(mongoType string) string {
	// Coerce "id" bsonId types into text since Postgres doesn't have type for BSONID
	switch strings.ToLower(mongoType) {
	case "id":
		return "text"
	}
	return mongoType
}

func normalizeDotNotationToPostgresNaming(key string) string {
	re := regexp.MustCompile(`\\.`)
	return re.ReplaceAllString(key, "_")
}
