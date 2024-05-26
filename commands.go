package monresql

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

type commands struct{}

func (c *commands) CreateTableSQL() string {
	q := queries{}
	fmt.Print("-- Execute the following SQL to setup table in Postgres. Replace $USERNAME with the moresql user.")
	query := q.CreateMetadataTable()
	return query
}

func (c *commands) ValidateTablesAndColumns(fieldMap fieldsMap, pg *sqlx.DB) []string {
	var results []string
	q := queries{}
	missingColumns := []tableColumn{}
	// Validates configuration of Postgres based on config file
	// Only validates SELECT and column existance
	for _, db := range fieldMap {
		for _, coll := range db.Collections {
			table := coll.PgTable
			// TODO: allow for non-public schema
			schema := "public"
			// Check that all columns are present
			rows, err := pg.NamedQuery(q.GetColumnsFromTable(), map[string]interface{}{"schema": schema, "table": table})
			if err != nil {
				log.Error(err)
			}
			// TODO: add validation that column types equal the types present in config

			resultMap := make(map[string]string)
			for rows.Next() {
				var row columnResult
				err := rows.StructScan(&row)
				if err != nil {
					log.Println(err)
				}
				resultMap[row.Name] = row.Name
			}

			for _, field := range coll.Fields {
				k := field.Postgres.Name
				_, ok := resultMap[k]
				if !ok {
					t := tableColumn{Schema: schema, Table: table, Column: k, Message: "Missing Column", Type: field.Postgres.Type}
					t.Solution = t.createColumn()
					missingColumns = append(missingColumns, t)
				}
			}

			// Check that each table has _id as in a unique index
			r := hasUniqueIndex{}
			err = pg.Get(&r, q.GetTableColumnIndexMetadata(), table, "_id")
			if err != nil {
				log.Error(err)
			}

			if !r.isValid() {
				t := tableColumn{Schema: schema, Table: table, Column: "_id", Message: "Missing Unique Index on Column", Type: ""}
				t.Solution = t.uniqueIndex()
				missingColumns = append(missingColumns, t)
			}

		}
	}
	if len(missingColumns) != 0 {
		log.Print("The following errors were reported:")
		tables := make(map[string]tableColumn)
		for _, v := range missingColumns {
			log.Printf("Table %s.%s Column: %s, Error: %s", v.Schema, v.Table, v.Column, v.Message)
			tables[v.Table] = v
		}
		log.Println("SQL Output to assist with correcting table schema malformation:")
		for _, v := range tables {
			fmt.Printf("CREATE TABLE IF NOT EXISTS %s.%s();\n", v.Schema, v.Table)
			Serr := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s();\n", v.Schema, v.Table)
			results = append(results, Serr)
		}
		// Column level advice
		for _, v := range missingColumns {
			fmt.Printf("%s\n", v.Solution)
			Serr := fmt.Sprintf("%s\n", v.Solution)
			results = append(results, Serr)
		}
		return results
	}
	result := fmt.Sprintln("Validation succeeded. Postgres tables look good.")
	results = append(results, result)

	return results
}
