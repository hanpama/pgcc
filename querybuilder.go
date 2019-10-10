package pgcc

import (
	"bytes"
	"text/template"
)

// PageInfo https://facebook.github.io/relay/graphql/connections.htm#sec-undefined.PageInfo
type PageInfo struct {
	HasNextPage     bool        `json:"has_next_page"`
	HasPreviousPage bool        `json:"has_previous_page"`
	StartCursor     interface{} `json:"start_cursor"`
	EndCursor       interface{} `json:"end_cursor"`
}

// Receive implements Receiver
func (pi *PageInfo) Receive() []interface{} {
	return []interface{}{&pi.HasNextPage, &pi.HasPreviousPage, &pi.StartCursor, &pi.EndCursor}
}

// Options defines required and optional settings for building connection query
type Options struct {
	// Table to paginate
	TableName string
	// Columns To Select
	Select string
	// Column used for cursor
	Cursor string
	// Options for sorting the table
	SortKeys []SortKey

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
	EdgesSQL,
	PageInfoSQL,
	TotalCountSQL string
}

// NewQueryBuilder creates a new QueryBuilder with given options
func NewQueryBuilder(options Options) *QueryBuilder {
	var qs QueryBuilder
	var res bytes.Buffer
	if err := tmpl.ExecuteTemplate(&res, "Edges", options); err != nil {
		panic(err)
	}
	qs.EdgesSQL = res.String()

	res.Reset()
	if err := tmpl.ExecuteTemplate(&res, "PageInfo", options); err != nil {
		panic(err)
	}
	qs.PageInfoSQL = res.String()

	res.Reset()
	if err := tmpl.ExecuteTemplate(&res, "TotalCount", options); err != nil {
		panic(err)
	}
	qs.TotalCountSQL = res.String()

	return &qs
}

// Edges creates a new Query for edges
func (qb *QueryBuilder) Edges(args Args) *Query {
	return newQuery(qb.EdgesSQL, args)
}

// PageInfo creates a new Query for pageInfo
func (qb *QueryBuilder) PageInfo(args Args) *Query {
	return newQuery(qb.PageInfoSQL, args)
}

// TotalCount creates a new TotalCountQuery
func (qb *QueryBuilder) TotalCount(args Args) *Query {
	return newQuery(qb.TotalCountSQL, args)
}

func newQuery(sql string, args []interface{}) *Query {
	return &Query{sql, args}
}

// Query is for querying pages for connection
type Query struct {
	sql  string
	args []interface{}
}

// SQL returns this query's SQL query as string
func (q *Query) SQL() string { return q.sql }

// Args returns bound arguments
func (q *Query) Args() []interface{} { return q.args }

var tmpl = template.Must(template.New("ConnectionTemplate").Parse(`
{{- define "TotalCount" -}}
WITH {{template "params"}}, {{template "afterAndBefore" .}}
SELECT count(*) FROM {{.TableName}} {{.Join}} WHERE {{if .Condition}}({{.Condition}}){{else}}TRUE{{end}}
{{- end -}}

{{- define "Edges"}}
WITH {{template "params"}}, {{template "afterAndBefore" .}}
SELECT {{.Select}} FROM ({{template "edges" .}}) __edges__
{{ end }}

{{- define "edges" -}}
WITH __forward_edges__ AS (
	{{template "forwardEdges" .}}
), __backward_edges__ AS (
	{{template "backwardEdges" .}}
)
SELECT * FROM __forward_edges__
UNION
SELECT * FROM __backward_edges__
ORDER BY {{range $i, $key := .SortKeys}}{{if $i}}, {{end}}{{$key.Name}} {{$key.Order -}}{{- end}}
OFFSET (
	CASE ($1 > 0 AND $3 > 0)
		WHEN TRUE
		THEN GREATEST(COALESCE(0 - $3 + (SELECT count(*) FROM __forward_edges__), 0), 0)
		ELSE 0
	END
)
{{- end -}}

{{- define "PageInfo" -}}
WITH {{template "params"}}, {{template "afterAndBefore" .}}, __edges__  AS (
	{{ template "edges" .}}
)
SELECT
	({{template "hasNextPage" .}}) AS has_next_page,
	({{template "hasPreviousPage" .}}) AS has_previous_page,
	(SELECT __cursor__ FROM __edges__ LIMIT 1) AS start_cursor,
	(SELECT __cursor__ FROM __edges__ OFFSET (SELECT count(*) - 1 FROM __edges__) LIMIT 1) AS start_cursor
{{ end -}}

{{- define "forwardEdges" -}}
	SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
	WHERE NOT ($1 IS NULL AND $3 IS NOT NULL)
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
		{{- end}}
	LIMIT $1
{{- end -}}

{{- define "backwardEdges" -}}
	SELECT {{template "selections" .}} FROM {{.TableName}} {{.Join}}
	WHERE $1 IS NULL AND $3 IS NOT NULL
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
		{{- end}}
	LIMIT $3
{{- end -}}

{{- define "hasPreviousPage" -}}
CASE
	WHEN $3 IS NOT NULL then (
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
{{- end -}}

{{- define "hasNextPage" -}}
CASE
	WHEN $1 IS NOT NULL then (
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
{{- end -}}

{{- define "afterPredicate"}}
	{{- range $i, $key := .SortKeys }}
		{{- if $i}} OR ({{end -}}
		{{- range $j, $key := $.SortKeys -}}
		  {{- if ge $i $j -}}
				{{- if $j}} AND {{end -}}
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
				{{- if $j}} AND {{end -}}
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
				{{- if $j}} AND {{end -}}
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
				{{- if $j}} AND {{end -}}
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
{{range $i, $key := .SortKeys}}{{if $i}}, {{end}}{{$key.Select}} AS {{$key.Name}}{{end}}, {{.Cursor}} AS __cursor__, {{.Select}}
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
	SELECT $1::int, $3::int
)
{{- end -}}
`))
