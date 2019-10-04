package pgcc

import (
	"bytes"
	"text/template"
)

// PageInfo https://facebook.github.io/relay/graphql/connections.htm#sec-undefined.PageInfo
type PageInfo struct {
	HasNextPage     bool    `json:"has_next_page"`
	HasPreviousPage bool    `json:"has_previous_page"`
	StartCursor     *string `json:"start_cursor"`
	EndCursor       *string `json:"end_cursor"`
}

// Options defines required and optional settings for building connection query
type Options struct {
	// Table to paginate
	TableName string
	// Column used for cursor
	Cursor string
	// Options for sorting the table
	SortKeys []SortKey

	// Additional expressions to select besides cursor
	Select string
	// Optional condition expression
	Condition string
	// Optional Join clause
	Join string
	// Optional GROUP BY clause
	GroupBy string
}

// SortKey defines sort order as select and order
type SortKey struct {
	Order  string
	Select string
}

// Name is used as alias in SQL query
func (sk SortKey) Name() string {
	return `"` + sk.Select + "_" + sk.Order + `"`
}

// QueryBuilder builds query which paginates SQL query
// with cursor and count parameters
type QueryBuilder struct {
	sqlConnection,
	sqlTotalCount string
}

// NewQueryBuilder creates a new QueryBuilder with given options
func NewQueryBuilder(options Options) *QueryBuilder {
	var qs QueryBuilder
	var res bytes.Buffer
	if err := tmpl.ExecuteTemplate(&res, "Connection", options); err != nil {
		panic(err)
	}
	qs.sqlConnection = res.String()

	res.Reset()
	if err := tmpl.ExecuteTemplate(&res, "TotalCount", options); err != nil {
		panic(err)
	}
	qs.sqlTotalCount = res.String()

	return &qs
}

// Paginate creates a new PaginationQuery
func (qb *QueryBuilder) Paginate() *PaginationQuery {
	return &PaginationQuery{
		qb.sqlConnection,
		map[int]interface{}{0: nil, 1: nil, 2: nil, 3: nil},
	}
}

// TotalCount creates a new TotalCountQuery
func (qb *QueryBuilder) TotalCount() *TotalCountQuery {
	return &TotalCountQuery{qb.sqlTotalCount}
}

// PaginationQuery is for querying pages for connection
type PaginationQuery struct {
	sql  string
	args map[int]interface{}
}

// SQL returns this query's SQL query as string
func (q *PaginationQuery) SQL() string { return q.sql }

// Args returns bound arguments
func (q *PaginationQuery) Args() (args []interface{}) {
	for i := 0; true; i++ {
		if arg, ok := q.args[i]; ok {
			args = append(args, arg)
		} else {
			break
		}
	}
	return args
}

// SetFirst sets the query's parameter `first` as given value
func (q *PaginationQuery) SetFirst(count int32) { q.args[0] = count }

// SetAfter sets the query's parameter `after` as given value
func (q *PaginationQuery) SetAfter(cursor string) { q.args[1] = cursor }

// SetLast sets the query's parameter `last` as given value
func (q *PaginationQuery) SetLast(count int32) { q.args[2] = count }

// SetBefore sets the query's parameter `before` as given value
func (q *PaginationQuery) SetBefore(cursor string) { q.args[3] = cursor }

// TotalCountQuery is for querying total count of all the edges it paginates
type TotalCountQuery struct {
	sql string
}

// SQL returns this query's SQL query as string
func (q *TotalCountQuery) SQL() string { return q.sql }

// Args returns bound arguments
func (q *TotalCountQuery) Args() []interface{} { return nil }

var tmpl = template.Must(template.New("ConnectionTemplate").Parse(`
{{- define "Connection" -}}
WITH {{ template "afterAndBefore" . }}
SELECT
  json_build_object(
		'edges', __edges__.result,
		'has_next_page', ({{ template "hasNextPage" . }}),
		'has_previous_page', ({{ template "hasPreviousPage" . }}),
		'start_cursor', CAST(__edges__.result ->> 0 AS JSON) ->> 'cursor',
		'end_cursor', CAST(__edges__.result ->> json_array_length(__edges__.result)-1 AS JSON) ->> 'cursor'
	)
FROM ({{ template "edges" . }}) __edges__
{{- end -}}

{{- define "TotalCount" -}}
SELECT count(*) FROM {{.TableName}} {{.Join}} WHERE {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}}
{{- end -}}

{{- define "edges" -}}
WITH __forward_edges__ AS (
	SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
	WHERE NOT (($1 + 0) IS NULL AND ($3 + 0) IS NOT NULL)
		AND {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}}
		AND CASE (SELECT TRUE FROM __after__)
			WHEN TRUE THEN ({{template "afterPredicate" .}})
			ELSE TRUE
		END
		AND CASE (SELECT TRUE FROM __before__)
			WHEN TRUE THEN ({{template "beforePredicate" .}})
			ELSE TRUE
		END
	{{.GroupBy}}
	ORDER BY
		{{- range $i, $key := .SortKeys}}{{- if $i}}, {{end}}
		{{$key.Name}} {{if eq $key.Order "ASC"}}ASC{{else}}DESC{{end}}
		{{- end}}, {{.Cursor}} ASC
	LIMIT $1
), __backward_edges__ AS (
	SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
	WHERE $1 + 0 IS NULL AND $3 > 0
		AND {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}}
		AND CASE (SELECT TRUE FROM __after__)
			WHEN TRUE THEN ({{template "afterPredicate" .}})
			ELSE TRUE
		END
		AND CASE (SELECT TRUE FROM __before__)
			WHEN TRUE THEN ({{template "beforePredicate" .}})
			ELSE TRUE
		END
	{{.GroupBy}}
	ORDER BY
		{{- range $i, $key := .SortKeys}}{{- if $i}}, {{end}}
		{{$key.Name}} {{if eq $key.Order "ASC"}}DESC{{else}}ASC{{end}}
		{{- end}}, {{.Cursor}} DESC
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
		ORDER BY {{range $i, $key := .SortKeys}}{{if $i}}, {{end}}{{$key.Name}} {{$key.Order -}}{{- end}}, cursor ASC
	) as __rawedges__
) __edgerows__
{{- end -}}

{{- define "hasPreviousPage" -}}
SELECT (
	CASE
		WHEN $3 > 0 then (
			WITH __limit_or_more__ AS (
				SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
				WHERE {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}}
					AND CASE (SELECT TRUE FROM __after__)
						WHEN TRUE THEN ({{template "afterPredicate" .}})
						ELSE TRUE
					END
					AND CASE (SELECT TRUE FROM __before__)
						WHEN TRUE THEN ({{template "beforePredicate" .}})
						ELSE TRUE
					END
				{{.GroupBy}} LIMIT $3 + 1
			)
			SELECT count(*) > $3 FROM __limit_or_more__
		)
		WHEN $2 IS NOT NULL then (
			WITH __zero_or_one__ AS (
				SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
				WHERE {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}}
					AND CASE (SELECT TRUE FROM __after__)
						WHEN TRUE THEN ({{template "beforeAfterPredicate" .}})
						ELSE TRUE
					END
				{{.GroupBy}} LIMIT 1
			)
			SELECT COUNT(*) > 0 FROM __zero_or_one__
		)
		ELSE FALSE
	END
) AS result
{{- end -}}

{{- define "hasNextPage" -}}
SELECT (
	CASE
		WHEN $1 > 0 then (
			WITH __limit_or_more__ AS (
				SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
				WHERE {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}}
					AND CASE (SELECT TRUE FROM __after__)
						WHEN TRUE THEN ({{template "afterPredicate" .}})
						ELSE TRUE
					END
					AND CASE (SELECT TRUE FROM __before__)
						WHEN TRUE THEN ({{template "beforePredicate" .}})
						ELSE TRUE
					END
					{{.GroupBy}} LIMIT $1 + 1
			)
			SELECT count(*) > $1 FROM __limit_or_more__
		)
		WHEN $4 IS NOT NULL then (
			WITH __zero_or_one__ AS (
				SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
				WHERE {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}}
					AND CASE (SELECT TRUE FROM __before__)
						WHEN TRUE THEN ({{template "afterBeforePredicate" .}})
						ELSE TRUE
					END
				{{.GroupBy}} LIMIT 1
			)
			SELECT COUNT(*) > 0 FROM __zero_or_one__
		)
		ELSE FALSE
	END
) AS result
{{- end -}}

{{- define "afterPredicate"}}
	{{- range $i, $key := .SortKeys }}
		{{- if $i}} OR ({{end -}}
		{{- range $j, $key := $.SortKeys -}}
		  {{- if ge $i $j -}}
				{{- if $j}}AND {{end -}}
				{{- if gt $i $j}}
					{{- $key.Select}} = (select {{$key.Name}} FROM __after__)
				{{- else}}
					{{- $key.Select}} {{if eq $key.Order "ASC"}}>{{else}}<{{end}} (select {{$key.Name}} FROM __after__)
				{{- end}}
			{{- end}}
		{{- end}}{{if $i}}){{end}}
	{{- end}}
{{- end}}

{{- define "beforePredicate"}}
	{{- range $i, $key := .SortKeys }}
		{{- if $i}} OR ({{end -}}
		{{- range $j, $key := $.SortKeys -}}
		  {{- if ge $i $j -}}
				{{- if $j}}AND {{end -}}
				{{- if gt $i $j}}
					{{- $key.Select}} = (select {{$key.Name}} FROM __before__)
				{{- else}}
					{{- $key.Select}} {{if eq $key.Order "ASC"}}<{{else}}>{{end}} (select {{$key.Name}} FROM __before__)
				{{- end}}
			{{- end}}
		{{- end}}{{if $i}}){{end}}
	{{- end}}
{{- end}}

{{- define "beforeAfterPredicate"}}
	{{- range $i, $key := .SortKeys }}
		{{- if $i}} OR ({{end -}}
		{{- range $j, $key := $.SortKeys -}}
		  {{- if ge $i $j -}}
				{{- if $j}}AND {{end -}}
				{{- if gt $i $j}}
					{{- $key.Select}} = (select {{$key.Name}} FROM __after__)
				{{- else}}
					{{- $key.Select}} {{if eq $key.Order "ASC"}}<{{else}}>{{end}} (select {{$key.Name}} FROM __after__)
				{{- end}}
			{{- end}}
		{{- end}}{{if $i}}){{end}}
	{{- end}}
{{- end}}

{{- define "afterBeforePredicate"}}
	{{- range $i, $key := .SortKeys }}
		{{- if $i}} OR ({{end -}}
		{{- range $j, $key := $.SortKeys -}}
		  {{- if ge $i $j -}}
				{{- if $j}}AND {{end -}}
				{{- if gt $i $j}}
					{{- $key.Select}} = (select {{$key.Name}} FROM __before__)
				{{- else}}
					{{- $key.Select}} {{if eq $key.Order "ASC"}}>{{else}}<{{end}} (select {{$key.Name}} FROM __before__)
				{{- end}}
			{{- end}}
		{{- end}}{{if $i}}){{end}}
	{{- end}}
{{- end}}

{{- define "selections" -}}
{{range $i, $key := .SortKeys}}{{if $i}}, {{end}}{{$key.Select}} AS {{$key.Name}}{{end}}, {{.Cursor}} AS cursor{{if .Select}}, {{.Select}}{{end}}
{{- end -}}

{{- define "afterAndBefore" -}}
__after__ AS (
	SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
	WHERE {{.Cursor}} = $2 AND {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}} {{.GroupBy}} LIMIT 1
), __before__ AS (
	SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
	WHERE {{.Cursor}} = $4 AND {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}} {{.GroupBy}} LIMIT 1
)
{{- end -}}

{{- define "params" -}}
__params__ AS (
	SELECT $1::int, $2, $3::int, $4
)
{{- end -}}
`))
