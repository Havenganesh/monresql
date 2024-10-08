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
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/rwynn/gtm/v2"
)

func isInsertUpdateDelete(op *gtm.Op) bool {
	return isActionableOperation(op.IsInsert, op.IsUpdate, op.IsDelete)
}

func isActionableOperation(filters ...func() bool) bool {
	for _, fn := range filters {
		if fn() {
			return true
		}
	}
	return false
}

// SanitizeData handles type inconsistency between mongo and pg
// and flattens the data from a potentially nested data struct
// into a flattened struct using gjson.
func sanitizeData(pgFields fields, op *gtm.Op) map[string]interface{} {
	if !isInsertUpdateDelete(op) {
		return make(map[string]interface{})
	}

	newData, err := json.Marshal(op.Data)
	parsed := gjson.ParseBytes(newData)
	output := make(map[string]interface{})
	if err != nil {
		log.Errorf("Failed to marshal op.Data into json %s", err.Error())
	}

	for k, v := range pgFields {
		// Dot notation extraction
		maybe := parsed.Get(k)
		if !maybe.Exists() {
			// Fill with nils to ensure that NamedExec works
			output[v.Postgres.Name] = nil
		} else {
			// Sanitize the Value field when it's a map
			value := maybe.Value()
			if _, ok := maybe.Value().(map[string]interface{}); ok {
				// Marshal Objects using JSON
				b, _ := json.Marshal(value)
				output[v.Postgres.Name] = string(b)
			} else if _, ok := maybe.Value().([]interface{}); ok {
				// Marshal Arrays using JSON
				b, _ := json.Marshal(value)
				output[v.Postgres.Name] = string(b)
			} else {
				output[v.Postgres.Name] = value
			}
		}
	}
	// log.Println("opId : ", op.Id, " : ", reflect.TypeOf(op.Id))
	if op.Id != nil {
		switch op.Id.(type) {
		case primitive.ObjectID:
			bid := op.Id.(primitive.ObjectID)
			output["_id"] = bid.Hex()
		default:
			output["_id"] = op.Id
		}
	}

	return output
}

func createFanKey(db string, collection string) string {
	return db + "." + collection
}

// EnsureOpHasAllFields: Ensure that required keys are present will null value
func ensureOpHasAllFields(op *gtm.Op, keysToEnsure []string) *gtm.Op {
	// Guard against assignment into nil map
	if op.Data == nil {
		op.Data = make(map[string]interface{})
	}
	for _, k := range keysToEnsure {
		if _, ok := op.Data[k]; !ok {
			op.Data[k] = nil
		}
	}
	return op
}

// func psqlUserNameSplitted(urlStr string) string {
// 	parsedURL, err := url.Parse(urlStr)
// 	if err != nil {
// 		fmt.Println("Error parsing URL:", err)
// 		return ""
// 	}
// 	return parsedURL.User.Username()
// }

func newPrimitiveTimeStamp(t time.Time, c uint32) (primitive.Timestamp, error) {
	return primitive.Timestamp{T: uint32(t.Unix()), I: c}, nil
}
