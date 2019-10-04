package pgcc_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hanpama/pgcc"
	_ "github.com/lib/pq"
)

func TestQueryBuilder(t *testing.T) {
	var err error
	tx := initTx()
	qb := pgcc.NewQueryBuilder(pgcc.Options{
		TableName: "town_test", // Table to query
		Cursor:    "id",        // should be a key in the table
		Select:    "created",   // additional columns to select
		SortKeys: []pgcc.SortKey{ // defines the sort orders for this connection
			{Order: "DESC", Select: "created"},
			{Order: "ASC", Select: "id"},
		},
	})

	type townEdge struct {
		Cursor  string `json:"cursor"`
		Created string `json:"created"`
	}

	type townConnection struct {
		pgcc.PageInfo
		Edges []townEdge `json:"edges"`
	}

	q := qb.Paginate()
	q.SetFirst(2)

	var src townConnection
	var b []byte
	err = tx.QueryRow(q.SQL(), q.Args()...).Scan(&b)
	if err != nil {
		t.Fatal(err)
	}

	err = json.Unmarshal(b, &src)
	if err != nil {
		t.Fatal(err)
	}

	if len(src.Edges) != 2 {
		t.Fatal("Edges should have length of 2 when first two queried")
	}

	err = tx.Rollback()
	if err != nil {
		panic(err)
	}
}

func initTx() *sql.Tx {
	db, err := sql.Open("postgres", os.Getenv("DB_URL"))
	if err != nil {
		panic(err)
	}
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	_, err = tx.Exec(`
		CREATE TABLE town_test (
			id TEXT PRIMARY KEY,
			created TIMESTAMPTZ NOT NULL
		)
	`)
	if err != nil {
		panic(err)
	}
	now := time.Now()
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("Town-%d", i)
		created := now.Add(time.Duration(i) * time.Hour)
		_, err = tx.Exec(`INSERT INTO town_test (id, created) VALUES ($1, $2)`, id, created)
		if err != nil {
			panic(err)
		}
	}
	return tx
}
