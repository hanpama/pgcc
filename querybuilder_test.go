package pgcc_test

import (
	"database/sql"
	"testing"

	"github.com/hanpama/pgcc"
	_ "github.com/lib/pq"
)

func TestQueryBuilder(t *testing.T) {

	tx := initTx()
	qb := pgcc.NewQueryBuilder(pgcc.Options{
		TableName: "test_post",   // Table to query
		Select:    "id, created", // columns to select
		Cursor:    "id",          // primary key
		SortKeys: []pgcc.SortKey{ // defines the sort orders for this connection
			{Order: "DESC", Select: "created"},
			{Order: "ASC", Select: "id"},
		},
	})

	args := pgcc.NewArgs(nil, nil, nil, nil)

	var totalCount int
	err := tx.QueryRow(qb.TotalCountSQL, args...).Scan(&totalCount)
	if err != nil {
		panic(err)
	}
	assertDeepEqual(t, totalCount, 200)

	args.SetFirst(5)
	edges := mustQueryEdges(tx, qb.EdgesSQL, args...)
	testJSONSnapshot(t, "first-5-edges", edges)
	pageInfo := mustQueryPageInfo(tx, qb.PageInfoSQL, args...)
	testJSONSnapshot(t, "first-5-pageInfo", pageInfo)

	args.SetAfter(196)
	edges = mustQueryEdges(tx, qb.EdgesSQL, args...)
	testJSONSnapshot(t, "first-5-after-196-edges", edges)

	args = pgcc.NewArgs(nil, nil, nil, nil)
	args.SetLast(10)
	edges = mustQueryEdges(tx, qb.EdgesSQL, args...)
	testJSONSnapshot(t, "last-10-edges", edges)

	args.SetBefore(10)
	edges = mustQueryEdges(tx, qb.EdgesSQL, args...)
	testJSONSnapshot(t, "last-10-before-10-edges", edges)

}

func mustQueryEdges(tx *sql.Tx, sql string, args ...interface{}) (edges Edges) {
	rows, err := tx.Query(sql, args...)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		err = rows.Scan(edges.Receive()...)
		if err != nil {
			panic(err)
		}
	}
	return edges
}

func mustQueryPageInfo(tx *sql.Tx, sql string, args ...interface{}) (pi pgcc.PageInfo) {
	err := tx.QueryRow(sql, args...).Scan(pi.Receive()...)
	if err != nil {
		panic(err)
	}
	return pi
}

type Edge struct {
	ID      int32  `json:"id"`
	Created string `json:"created"`
}

func (e *Edge) Receive() []interface{} {
	return []interface{}{&e.ID, &e.Created}
}

type Edges []Edge

func (rs *Edges) Receive() []interface{} {
	*rs = append(*rs, Edge{})
	return (*rs)[len(*rs)-1].Receive()
}
