package monresql

import (
	"encoding/json"
	"fmt"
	"regexp"

	"strings"

	log "github.com/sirupsen/logrus"
)

// LoadFieldsMap receive the file as json string and return FieldsMap
// please refer the moresql config file structure
func loadFieldsMap(jsonString string) (FieldsMap, error) {
	config, err := jsonToFieldsMap(jsonString)
	if err != nil {
		fmt.Printf("Error While Validation : %s", err)
		return config, err
	}
	return config, nil
}

func jsonToFieldsMap(s string) (FieldsMap, error) {
	config := FieldsMap{}
	var configDelayed ConfigDelayed
	err := json.Unmarshal([]byte(s), &configDelayed)
	if err != nil {
		log.Println("LoadConfig String ", err)
		return config, err
	}
	for k, v := range configDelayed {
		db := DB{}
		collections := Collections{}
		db.Collections = collections
		for k, v := range v.Collections {
			coll := Collection{Name: v.Name, PgTable: v.PgTable}
			var fields Fields
			fields, err = jsonToFields(string(v.Fields))
			if err != nil {
				log.Warnf("JSON Config decoding error: %s", err)
				return nil, fmt.Errorf("unable to decode %w", err)
			}
			coll.Fields = fields
			db.Collections[k] = coll
		}
		config[k] = db
	}
	return config, nil
}

func jsonToFields(s string) (Fields, error) {
	var init FieldsWrapper
	var err error
	result := Fields{}
	err = json.Unmarshal([]byte(s), &init)
	for k, v := range init {
		field := Field{}
		str := ""
		if err := json.Unmarshal(v, &field); err == nil {
			result[k] = field
		} else if err := json.Unmarshal(v, &str); err == nil {
			// Convert shorthand to longhand Field
			f := Field{
				Mongo{k, str},
				Postgres{normalizeDotNotationToPostgresNaming(k), mongoToPostgresTypeConversion(str)},
			}
			result[k] = f
		} else {
			errLong := json.Unmarshal(v, &field)
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
