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

// Queries contains the sql commands used by Monresql
type queries struct{}

// GetMetadata fetches the most recent metadata row for this appname
func (q *queries) GetMetadata() string {
	return `SELECT * FROM monresql_metadata WHERE app_name=$1 ORDER BY last_epoch DESC LIMIT 1;`
}

// SaveMetadata performs an upsert using metadata with uniqueness constraint on app_name
func (q *queries) SaveMetadata() string {
	return `INSERT INTO "monresql_metadata" ("app_name", "last_epoch", "processed_at")
VALUES (:app_name, :last_epoch, :processed_at)
ON CONFLICT ("app_name")
DO UPDATE SET "last_epoch" = :last_epoch, "processed_at" = :processed_at;`
}

// CreateMetadataTable provides the sql required to setup the metadata table
func (q *queries) CreateMetadataTable() string {
	return `
-- create the monresql_metadata table for checkpoint persistance
CREATE TABLE public.monresql_metadata
(
    app_name TEXT NOT NULL,
    last_epoch INT NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);
-- Setup mandatory unique index
CREATE UNIQUE INDEX monresql_metadata_app_name_uindex ON public.monresql_metadata (app_name);

-- Grant permissions to this user, replace username with moresql's user
GRANT SELECT, UPDATE, DELETE ON TABLE public.monresql_metadata TO $USERNAME;

COMMENT ON COLUMN public.monresql_metadata.app_name IS 'Name of application. Used for circumstances where multiple apps stream to same PG instance.';
COMMENT ON COLUMN public.monresql_metadata.last_epoch IS 'Most recent epoch processed from Mongo';
COMMENT ON COLUMN public.monresql_metadata.processed_at IS 'Timestamp for when the last epoch was processed at';
COMMENT ON TABLE public.monresql_metadata IS 'Stores checkpoint data for Monresql (mongo->pg) streaming';
`
}

func (q *queries) GetColumnsFromTable() string {
	return `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = :schema
  AND table_name   = :table`
}

func (q *queries) GetTableColumnIndexMetadata() string {
	return `
-- Get table, columns, and index metadata
WITH tables_and_indexes AS (
  -- CREDIT: http://stackoverflow.com/a/25596855
    SELECT
      c.relname                                       AS table,
      f.attname                                       AS column,
      pg_catalog.format_type(f.atttypid, f.atttypmod) AS type,
      f.attnotnull                                    AS notnull,
      i.relname                                       AS index_name,
      CASE
      WHEN i.oid <> 0
        THEN TRUE
      ELSE FALSE
      END                                             AS is_index,
      CASE
      WHEN p.contype = 'p'
        THEN TRUE
      ELSE FALSE
      END                                             AS primarykey,
      CASE
      WHEN p.contype = 'u'
        THEN TRUE
      WHEN p.contype = 'p'
        THEN TRUE
      ELSE FALSE
      END                                             AS uniquekey
    FROM pg_attribute f
      JOIN pg_class c ON c.oid = f.attrelid
      JOIN pg_type t ON t.oid = f.atttypid
      LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
      LEFT JOIN pg_class AS g ON p.confrelid = g.oid
      LEFT JOIN pg_index AS ix ON f.attnum = ANY (ix.indkey) AND c.oid = f.attrelid AND c.oid = ix.indrelid
      LEFT JOIN pg_class AS i ON ix.indexrelid = i.oid

    WHERE c.relkind = 'r' :: CHAR
          AND n.nspname = 'public'  -- Replace with Schema name
          --AND c.relname = 'nodes'  -- Replace with table name, or Comment this for get all tables
          AND f.attnum > 0
    ORDER BY c.relname, f.attname
)
SELECT count(*) from tables_and_indexes
WHERE "table" = $1
AND "column" = $2
AND is_index IS TRUE
-- TODO: determine how to check if index is unique vs unique column
-- AND uniquekey IS TRUE;
	`
}
