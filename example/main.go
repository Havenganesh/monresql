package main

import "github.com/Havenganesh/monresql"

func main() {
	monresql.LoadFieldsMap("")
}

var jsonF string = `{
	"acme_project": {
	  "collections": {
		"Authors": {
		  "name": "Authors",
		  "pg_table": "authors",
		  "fields": {
			"_id": "TEXT",
			"addresses": "JSONB",
			"books": "JSONB",
			"books.#.product_id": {
			  "Postgres": {"Name": "book_ids","Type": "JSONB"},
			  "Mongo": {"Name": "books.#.product_id","Type": "object"}
			}
		  }
		},
		"Users": {
		  "name": "Users",
		  "pg_table": "users",
		  "fields": {
			"_id": "TEXT",
			"email": "TEXT",
			"name": "TEXT",
			"preferences": "JSONB",
			"preferences.unsubscribe": {
			  "Postgres": {"Name": "is_unsubscribed", "Type": "BOOLEAN"},
			  "Mongo": {"Name": "preferences.unsubscribe", "Type": "object"}
			},
			"createdAt": "TIMESTAMP WITH TIME ZONE"
		  }
		}
	  }
	}
  }`
