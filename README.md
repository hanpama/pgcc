# PGCC

Relay Cursor Connection implementation for Golang and PostgresQL

A connection contains its paginated edges and pagination info.

* A Cursor is a string which can uniquely identify the edge's position in the entire list.
* Each edge has a node as its target, so the node ids are good to be cursors.


```
town

id(pk)    created
-------   ----------
town-20   2019-09-06
town-19   2019-09-05
town-18   2019-09-04
town-17   2019-09-03
town-16   2019-09-02
town-15   2019-09-01
town-14   2019-08-31
town-13   2019-08-30
...
```

When querying table `town` order by `created DESC`, You can use `id` as its cursor.

Let's find the first 5 edges after `town-19`.

```
town

id        created
-------   ----------
town-20   2019-09-06
town-19   2019-09-05 <- pointed by `after`
town-18   2019-09-04 <- 1
town-17   2019-09-03 <- 2
town-16   2019-09-02 <- 3
town-15   2019-09-01 <- 4
town-14   2019-08-31 <- 5
town-13   2019-08-30
...
```

Its actual query is like:

```sql
SELECT id FROM town
WHERE created < (SELECT created FROM town WHERE id = 'town-19') -- maybe indexed?
ORDER BY created DESC
LIMIT 5
```


How about 'backward pagination' using `last` and `before`?

We are going to `ORDER` the rows `BY created ASC` but we should not reverse the order of edges in results.
(https://facebook.github.io/relay/graphql/connections.htm#sec-Edge-order)

For example, when we query last 3 edges before `town-4`:

```
town

id        created
-------   ----------
town-1    2019-08-03
town-2    2019-08-04
town-3    2019-08-05
town-4    2019-08-06 <- pointed by `before`
town-5    2019-08-07 <- 1
town-6    2019-08-08 <- 2
town-7    2019-08-09 <- 3
town-8    2019-08-10
...
```

The query will be like:

```sql
WITH __backward_edges__ AS (
  SELECT id FROM town
  WHERE created > (SELECT created FROM town WHERE id = 'town-4') -- still can use index
  ORDER BY created ASC
  LIMIT 3
)
SELECT * FROM __backward_edges__
ORDER BY created DESC
```

Because `town-7` should appear before `town-6`, we reversed the `__backward_edges__` here.

```go

qb := pgcc.NewQueryBuilder(pgcc.Options{
  TableName: "town",    // Table to query
  Cursor:    "id",      // should be a key in the table
  Select:    "created", // additional columns to select
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
  // ...
}
err = json.Unmarshal(b, &src)
if err != nil {
  // ...
}

```

It generates a query like:

```sql
WITH __after__ AS (
	SELECT created AS "created_DESC", id AS "id_ASC", id AS cursor, created FROM town
	WHERE id = $2 AND TRUE  LIMIT 1
), __before__ AS (
	SELECT created AS "created_DESC", id AS "id_ASC", id AS cursor, created FROM town
	WHERE id = $4 AND TRUE  LIMIT 1
)
SELECT
  json_build_object(
		'edges', __edges__.result,
		'has_next_page', (SELECT (
	CASE
		WHEN $1 > 0 then (
			WITH __limit_or_more__ AS (
				SELECT created AS "created_DESC", id AS "id_ASC", id AS cursor, created FROM town
				WHERE TRUE
					AND CASE (SELECT TRUE FROM __after__)
						WHEN TRUE THEN (created < (select "created_DESC" FROM __after__) OR (created = (select "created_DESC" FROM __after__)AND id > (select "id_ASC" FROM __after__)))
						ELSE TRUE
					END
					AND CASE (SELECT TRUE FROM __before__)
						WHEN TRUE THEN (created > (select "created_DESC" FROM __before__) OR (created = (select "created_DESC" FROM __before__)AND id < (select "id_ASC" FROM __before__)))
						ELSE TRUE
					END
					 LIMIT $1 + 1
			)
			SELECT count(*) > $1 FROM __limit_or_more__
		)
		WHEN $4 IS NOT NULL then (
			WITH __zero_or_one__ AS (
				SELECT created AS "created_DESC", id AS "id_ASC", id AS cursor, created FROM town
				WHERE TRUE
					AND CASE (SELECT TRUE FROM __before__)
						WHEN TRUE THEN (created < (select "created_DESC" FROM __before__) OR (created = (select "created_DESC" FROM __before__)AND id > (select "id_ASC" FROM __before__)))
						ELSE TRUE
					END
				 LIMIT 1
			)
			SELECT COUNT(*) > 0 FROM __zero_or_one__
		)
		ELSE FALSE
	END
) AS result),
		'has_previous_page', (SELECT (
	CASE
		WHEN $3 > 0 then (
			WITH __limit_or_more__ AS (
				SELECT created AS "created_DESC", id AS "id_ASC", id AS cursor, created FROM town
				WHERE TRUE
					AND CASE (SELECT TRUE FROM __after__)
						WHEN TRUE THEN (created < (select "created_DESC" FROM __after__) OR (created = (select "created_DESC" FROM __after__)AND id > (select "id_ASC" FROM __after__)))
						ELSE TRUE
					END
					AND CASE (SELECT TRUE FROM __before__)
						WHEN TRUE THEN (created > (select "created_DESC" FROM __before__) OR (created = (select "created_DESC" FROM __before__)AND id < (select "id_ASC" FROM __before__)))
						ELSE TRUE
					END
				 LIMIT $3 + 1
			)
			SELECT count(*) > $3 FROM __limit_or_more__
		)
		WHEN $2 IS NOT NULL then (
			WITH __zero_or_one__ AS (
				SELECT created AS "created_DESC", id AS "id_ASC", id AS cursor, created FROM town
				WHERE TRUE
					AND CASE (SELECT TRUE FROM __after__)
						WHEN TRUE THEN (created > (select "created_DESC" FROM __after__) OR (created = (select "created_DESC" FROM __after__)AND id < (select "id_ASC" FROM __after__)))
						ELSE TRUE
					END
				 LIMIT 1
			)
			SELECT COUNT(*) > 0 FROM __zero_or_one__
		)
		ELSE FALSE
	END
) AS result),
		'start_cursor', CAST(__edges__.result ->> 0 AS JSON) ->> 'cursor',
		'end_cursor', CAST(__edges__.result ->> json_array_length(__edges__.result)-1 AS JSON) ->> 'cursor'
	)
FROM (WITH __forward_edges__ AS (
	SELECT created AS "created_DESC", id AS "id_ASC", id AS cursor, created FROM town
	WHERE NOT (($1 + 0) IS NULL AND ($3 + 0) IS NOT NULL)
		AND TRUE
		AND CASE (SELECT TRUE FROM __after__)
			WHEN TRUE THEN (created < (select "created_DESC" FROM __after__) OR (created = (select "created_DESC" FROM __after__)AND id > (select "id_ASC" FROM __after__)))
			ELSE TRUE
		END
		AND CASE (SELECT TRUE FROM __before__)
			WHEN TRUE THEN (created > (select "created_DESC" FROM __before__) OR (created = (select "created_DESC" FROM __before__)AND id < (select "id_ASC" FROM __before__)))
			ELSE TRUE
		END

	ORDER BY
		"created_DESC" DESC,
		"id_ASC" ASC, id ASC
	LIMIT $1
), __backward_edges__ AS (
	SELECT created AS "created_DESC", id AS "id_ASC", id AS cursor, created FROM town
	WHERE $1 + 0 IS NULL AND $3 > 0
		AND TRUE
		AND CASE (SELECT TRUE FROM __after__)
			WHEN TRUE THEN (created < (select "created_DESC" FROM __after__) OR (created = (select "created_DESC" FROM __after__)AND id > (select "id_ASC" FROM __after__)))
			ELSE TRUE
		END
		AND CASE (SELECT TRUE FROM __before__)
			WHEN TRUE THEN (created > (select "created_DESC" FROM __before__) OR (created = (select "created_DESC" FROM __before__)AND id < (select "id_ASC" FROM __before__)))
			ELSE TRUE
		END

	ORDER BY
		"created_DESC" ASC,
		"id_ASC" DESC, id DESC
	LIMIT $3
)
SELECT COALESCE(JSON_AGG(__edgerows__.result), '[]'::json) AS result FROM (
	SELECT ROW_TO_JSON(__rawedges__.*) AS result FROM (
		(SELECT __forward_edges__.* FROM __forward_edges__
		OFFSET (
			CASE ($1 > 0 AND $3 > 0)
				WHEN TRUE
				THEN GREATEST(COALESCE(0 - $3 + (SELECT count(*) FROM __forward_edges__), 0), 0)
				ELSE 0
			END
		))
		UNION
		SELECT __backward_edges__.* FROM __backward_edges__
		ORDER BY "created_DESC" DESC, "id_ASC" ASC, cursor ASC
	) as __rawedges__
) __edgerows__) __edges__
```
