package pgcc_test

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kr/pretty"
	_ "github.com/lib/pq"
)

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
		CREATE TABLE test_post (
			id SERIAL PRIMARY KEY,
			created TIMESTAMPTZ NOT NULL
		)
	`)
	if err != nil {
		panic(err)
	}
	now, err := time.Parse(time.RFC3339, "2019-10-10T08:07:06.185Z")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 200; i++ {
		// id := fmt.Sprintf("Town-%d", i)
		created := now.Add(time.Duration(i) * time.Hour)
		_, err = tx.Exec(`INSERT INTO test_post (created) VALUES ($1)`, created)
		if err != nil {
			panic(err)
		}
	}
	return tx
}

func testJSONSnapshot(t *testing.T, name string, res interface{}) {
	resB, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}
	path := filepath.Join("__snapshots__", name+".json")
	path, err = filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	_, err = os.Stat(path)
	if os.IsNotExist(err) {

		ioutil.WriteFile(path, resB, os.ModePerm)
		t.Logf("Wrote snapshot: %s(%s)", name, path)
		return
	}
	snapshotB, err := ioutil.ReadFile(path)

	if string(resB) != string(snapshotB) {
		pretty.Log(string(resB), string(snapshotB))
		t.Logf("Snapshot %s doesn't match", name)
		t.Fail()
	}
}

func assertDeepEqual(t *testing.T, val interface{}, expected interface{}) {
	if !reflect.DeepEqual(val, expected) {
		t.Fatalf("Expected value to equal to %+v but got %+v", expected, val)
	}
}
