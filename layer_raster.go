package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	// Database
	"github.com/CrunchyData/pg_tileserv/cql"
	"github.com/jackc/pgtype"

	// Logging

	// Configuration
	"github.com/spf13/viper"
)

// LayerRaster provides metadata about the table layer
type LayerRaster struct {
	ID             string
	Schema         string
	Table          string
	Description    string
	Properties     map[string]RasterProperty
	GeometryType   string
	IDColumn       string
	GeometryColumn string
	Srid           int
}

// RasterProperty provides metadata about a single property field,
// features in a table layer may have multiple such fields
type RasterProperty struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	order       int
}

// RasterDetailJSON gives the output structure for the table layer.
type RasterDetailJSON struct {
	ID           string           `json:"id"`
	Schema       string           `json:"schema"`
	Name         string           `json:"name"`
	Description  string           `json:"description,omitempty"`
	Properties   []RasterProperty `json:"properties,omitempty"`
	GeometryType string           `json:"geometrytype,omitempty"`
	Center       [2]float64       `json:"center"`
	Bounds       [4]float64       `json:"bounds"`
	MinZoom      int              `json:"minzoom"`
	MaxZoom      int              `json:"maxzoom"`
	TileURL      string           `json:"tileurl"`
}

/********************************************************************************
 * Layer Interface
 */

// GetType disambiguates between function and table layers
func (lyr LayerRaster) GetType() LayerType {
	return LayerTypeRaster
}

// GetID returns the complete ID (schema.name) by which to reference a given layer
func (lyr LayerRaster) GetID() string {
	return lyr.ID
}

// GetDescription returns the text description for a layer
// or an empty string if no description is set
func (lyr LayerRaster) GetDescription() string {
	return lyr.Description
}

// GetName returns just the name of a given layer
func (lyr LayerRaster) GetName() string {
	return lyr.Table
}

// GetSchema returns just the schema for a given layer
func (lyr LayerRaster) GetSchema() string {
	return lyr.Schema
}

// WriteLayerJSON outputs parameters and optional arguments for the table layer
func (lyr LayerRaster) WriteLayerJSON(w http.ResponseWriter, req *http.Request) error {
	jsonTableDetail, err := lyr.getRasterDetailJSON(req)
	if err != nil {
		return err
	}
	w.Header().Add("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jsonTableDetail)
	// all good, no error
	return nil
}

// GetTileRequest takes tile and request parameters as input and returns a TileRequest
// specifying the SQL to fetch appropriate data
func (lyr LayerRaster) GetTileRequest(tile Tile, r *http.Request) TileRequest {

	// flip y to match the spec
	y := (1 << tile.Zoom) - 1 - tile.Y
	sql := fmt.Sprintf(`SELECT tile_data FROM "%s"."%s" 
						WHERE zoom_level = %d AND 
						tile_column = %d AND 
						tile_row = %d;`, lyr.Schema, lyr.Table, tile.Zoom, tile.X, y)

	tr := TileRequest{
		LayerID: lyr.ID,
		Tile:    tile,
		SQL:     sql,
		Args:    nil,
	}
	return tr
}

/********************************************************************************/

// getRequestPropertiesParameter compares the properties in the request
// with the properties in the table layer, and returns a slice of
// just those that occur in both, or a slice of all table properties
// if there is not query parameter, or no matches
func (lyr *LayerRaster) getQueryPropertiesParameter(q url.Values) []string {
	sAtts := make([]string, 0)
	haveProperties := false

	for k, v := range q {
		if strings.EqualFold(k, "properties") {
			sAtts = v
			haveProperties = true
			break
		}
	}

	lyrAtts := (*lyr).Properties
	queryAtts := make([]string, 0, len(lyrAtts))
	haveIDColumn := false

	if haveProperties {
		aAtts := strings.Split(sAtts[0], ",")
		for _, att := range aAtts {
			decAtt, err := url.QueryUnescape(att)
			if err == nil {
				decAtt = strings.Trim(decAtt, " ")
				att, ok := lyrAtts[decAtt]
				if ok {
					if att.Name == lyr.IDColumn {
						haveIDColumn = true
					}
					queryAtts = append(queryAtts, att.Name)
				}
			}
		}
	}
	// No request parameter or no matches, so we want to
	// return all the properties in the table layer
	if len(queryAtts) == 0 {
		for _, v := range lyrAtts {
			queryAtts = append(queryAtts, v.Name)
		}
	}
	if (!haveIDColumn) && lyr.IDColumn != "" {
		queryAtts = append(queryAtts, lyr.IDColumn)
	}
	return queryAtts
}

// getRequestParameters reads user-settables parameters
// from the request URL, or uses the system defaults
// if the parameters are not set
func (lyr *LayerRaster) getQueryParameters(q url.Values) queryParameters {
	rp := queryParameters{
		Limit:      getQueryIntParameter(q, "limit"),
		Resolution: getQueryIntParameter(q, "resolution"),
		Buffer:     getQueryIntParameter(q, "buffer"),
		Properties: lyr.getQueryPropertiesParameter(q),
		Filter:     getQueryStringParameter(q, "filter"),
		FilterCrs:  getQueryIntParameter(q, "filter-crs"),
	}
	if rp.Limit < 0 {
		rp.Limit = viper.GetInt("MaxFeaturesPerTile")
	}
	if rp.Resolution < 0 {
		rp.Resolution = viper.GetInt("DefaultResolution")
	}
	if rp.Buffer < 0 {
		rp.Buffer = viper.GetInt("DefaultBuffer")
	}
	if rp.FilterCrs < 0 {
		rp.FilterCrs = 4326
	}
	return rp
}

/********************************************************************************/

func (lyr *LayerRaster) getRasterDetailJSON(req *http.Request) (RasterDetailJSON, error) {
	td := RasterDetailJSON{
		ID:           lyr.ID,
		Schema:       lyr.Schema,
		Name:         lyr.Table,
		Description:  lyr.Description,
		GeometryType: lyr.GeometryType,
		MinZoom:      viper.GetInt("DefaultMinZoom"),
		MaxZoom:      viper.GetInt("DefaultMaxZoom"),
	}
	// TileURL is relative to server base
	td.TileURL = fmt.Sprintf("%s/%s/{z}/{x}/{y}.jpg", serverURLBase(req), url.PathEscape(lyr.ID))

	// Want to add the properties to the Json representation
	// in table order, which is fiddly
	tmpMap := make(map[int]RasterProperty)
	tmpKeys := make([]int, 0, len(lyr.Properties))
	for _, v := range lyr.Properties {
		tmpMap[v.order] = v
		tmpKeys = append(tmpKeys, v.order)
	}
	sort.Ints(tmpKeys)
	for _, v := range tmpKeys {
		td.Properties = append(td.Properties, tmpMap[v])
	}

	// Read table bounds and convert to Json
	// which prefers an array form
	bnds, err := lyr.GetBounds()
	if err != nil {
		return td, err
	}
	td.Bounds[0] = bnds.Xmin
	td.Bounds[1] = bnds.Ymin
	td.Bounds[2] = bnds.Xmax
	td.Bounds[3] = bnds.Ymax
	td.Center[0] = (bnds.Xmin + bnds.Xmax) / 2.0
	td.Center[1] = (bnds.Ymin + bnds.Ymax) / 2.0
	return td, nil
}

// GetBoundsExact returns the data coverage extent for a table layer
// in EPSG:4326, clipped to (+/-180, +/-90)
func (lyr *LayerRaster) GetBoundsExact() (Bounds, error) {
	bounds := Bounds{}
	extentSQL := fmt.Sprintf(`
	WITH ext AS (
		SELECT
			coalesce(
				ST_Transform(ST_SetSRID(ST_Extent("%s"), %d), 4326),
				ST_MakeEnvelope(-180, -90, 180, 90, 4326)
			) AS geom
		FROM "%s"."%s"
	)
	SELECT
		ST_XMin(ext.geom) AS xmin,
		ST_YMin(ext.geom) AS ymin,
		ST_XMax(ext.geom) AS xmax,
		ST_YMax(ext.geom) AS ymax
	FROM ext
	`, lyr.GeometryColumn, lyr.Srid, lyr.Schema, lyr.Table)

	db, err := dbConnect()
	if err != nil {
		return bounds, err
	}
	var (
		xmin pgtype.Float8
		xmax pgtype.Float8
		ymin pgtype.Float8
		ymax pgtype.Float8
	)
	err = db.QueryRow(context.Background(), extentSQL).Scan(&xmin, &ymin, &xmax, &ymax)
	if err != nil {
		return bounds, tileAppError{
			SrcErr:  err,
			Message: "Unable to calculate table bounds",
		}
	}

	bounds.SRID = 4326
	bounds.Xmin = xmin.Float
	bounds.Ymin = ymin.Float
	bounds.Xmax = xmax.Float
	bounds.Ymax = ymax.Float
	bounds.sanitize()
	return bounds, nil
}

// GetBounds returns the estimated extent for a table layer, transformed to EPSG:4326
func (lyr *LayerRaster) GetBounds() (Bounds, error) {
	bounds := Bounds{}
	// extentSQL := fmt.Sprintf(`
	// 	WITH ext AS (
	// 		SELECT ST_Transform(ST_SetSRID(ST_EstimatedExtent('%s', '%s', '%s'), %d), 4326) AS geom
	// 	)
	// 	SELECT
	// 		ST_XMin(ext.geom) AS xmin,
	// 		ST_YMin(ext.geom) AS ymin,
	// 		ST_XMax(ext.geom) AS xmax,
	// 		ST_YMax(ext.geom) AS ymax
	// 	FROM ext
	// 	`, lyr.Schema, lyr.Table, lyr.GeometryColumn, lyr.Srid)

	// db, err := dbConnect()
	// if err != nil {
	// 	return bounds, err
	// }

	// var (
	// 	xmin pgtype.Float8
	// 	xmax pgtype.Float8
	// 	ymin pgtype.Float8
	// 	ymax pgtype.Float8
	// )
	// err = db.QueryRow(context.Background(), extentSQL).Scan(&xmin, &ymin, &xmax, &ymax)
	// if err != nil {
	// 	return bounds, tileAppError{
	// 		SrcErr:  err,
	// 		Message: "Unable to calculate table bounds",
	// 	}
	// }

	// // Failed to get estimate? Get the exact bounds.
	// if xmin.Status == pgtype.Null {
	// 	warning := fmt.Sprintf("Estimated extent query failed, run 'ANALYZE %s.%s'", lyr.Schema, lyr.Table)
	// 	log.WithFields(log.Fields{
	// 		"event": "request",
	// 		"topic": "detail",
	// 		"key":   warning,
	// 	}).Warn(warning)
	// 	return lyr.GetBoundsExact()
	// }
	bounds.SRID = 4326
	bounds.Xmin = -180
	bounds.Ymin = -90
	bounds.Xmax = 180
	bounds.Ymax = 90
	bounds.sanitize()
	return bounds, nil
}

func (lyr *LayerRaster) requestSQL(tile *Tile, qp *queryParameters) (string, error) {

	type sqlParameters struct {
		TileSQL        string
		QuerySQL       string
		FilterSQL      string
		TileSrid       int
		Resolution     int
		Buffer         int
		Properties     string
		MvtParams      string
		Limit          string
		Schema         string
		Table          string
		GeometryColumn string
		Srid           int
	}

	// need both the exact tile boundary for clipping and an
	// expanded version for querying
	tileBounds := tile.Bounds
	queryBounds := tile.Bounds
	queryBounds.Expand(tile.width() * float64(qp.Buffer) / float64(qp.Resolution))
	tileSQL := tileBounds.SQL()
	tileQuerySQL := queryBounds.SQL()

	filterSQL, err := lyr.filterSQL(qp)
	if err != nil {
		return "", err
	}
	// SRID of the tile we are going to generate, which might be different
	// from the layer SRID in the database
	tileSrid := tile.Bounds.SRID

	// preserve case and special characters in column names
	// of SQL query by double quoting names
	attrNames := make([]string, 0, len(qp.Properties))
	for _, a := range qp.Properties {
		attrNames = append(attrNames, fmt.Sprintf("\"%s\"", a))
	}

	// only specify MVT format parameters we have configured
	mvtParams := make([]string, 0)
	mvtParams = append(mvtParams, fmt.Sprintf("'%s', %d", lyr.ID, qp.Resolution))
	if lyr.GeometryColumn != "" {
		mvtParams = append(mvtParams, fmt.Sprintf("'%s'", lyr.GeometryColumn))
	}
	// The idColumn parameter is PostGIS3+ only
	if globalPostGISVersion >= 3000000 && lyr.IDColumn != "" {
		mvtParams = append(mvtParams, fmt.Sprintf("'%s'", lyr.IDColumn))
	}

	sp := sqlParameters{
		TileSQL:        tileSQL,
		QuerySQL:       tileQuerySQL,
		FilterSQL:      filterSQL,
		TileSrid:       tileSrid,
		Resolution:     qp.Resolution,
		Buffer:         qp.Buffer,
		Properties:     strings.Join(attrNames, ", "),
		MvtParams:      strings.Join(mvtParams, ", "),
		Schema:         lyr.Schema,
		Table:          lyr.Table,
		GeometryColumn: lyr.GeometryColumn,
		Srid:           lyr.Srid,
	}

	if qp.Limit > 0 {
		sp.Limit = fmt.Sprintf("LIMIT %d", qp.Limit)
	}

	// TODO: Remove ST_Force2D when fixes to line clipping are common
	// in GEOS. See https://trac.osgeo.org/postgis/ticket/4690
	tmplSQL := `
	SELECT ST_AsMVT(mvtgeom, {{ .MvtParams }}) FROM (
		SELECT ST_AsMVTGeom(
			ST_Transform(ST_Force2D(t."{{ .GeometryColumn }}"), {{ .TileSrid }}),
			bounds.geom_clip,
			{{ .Resolution }},
			{{ .Buffer }}
		  ) AS "{{ .GeometryColumn }}"
		  {{ if .Properties }}
		  , {{ .Properties }}
		  {{ end }}
		FROM "{{ .Schema }}"."{{ .Table }}" t, (
			SELECT {{ .TileSQL }}  AS geom_clip,
					{{ .QuerySQL }} AS geom_query
			) bounds
		WHERE ST_Intersects(t."{{ .GeometryColumn }}",
							ST_Transform(bounds.geom_query, {{ .Srid }}))
			{{ .FilterSQL }}
		{{ .Limit }}
	) mvtgeom
	`

	sql, err := renderSQLTemplate("tabletilesql", tmplSQL, sp)
	if err != nil {
		return "", err
	}
	return sql, err
}

func (lyr *LayerRaster) filterSQL(qp *queryParameters) (string, error) {
	//filter := "pop_est < 2000000"
	filter := qp.Filter
	sql, err := cql.TranspileToSQL(filter, qp.FilterCrs, lyr.Srid)
	if err != nil {
		return "", err
	}
	if sql != "" {
		sql = "AND " + sql
	}
	return sql, nil
}

func getRasterLayers() ([]LayerRaster, error) {

	// layerSQL := `
	// SELECT
	// 	Format('public.%s', tablename) AS id,
	// 	'public' AS schema,
	// 	tablename AS table,
	// 	'description' AS description,
	// 	'geom' AS geometry_column,
	// 	4326 as srid,
	// 	'raster' AS geometry_type,
	// 	'xyz' AS id_column,
	// 	'{{zoom_level,int4,"",1},{tile_column,int4,"",2},{tile_row,int4,"",3},{tile_data,bytea,"",4}}' AS props
	// FROM pg_tables WHERE
	// 	schemaname = 'public' AND
	// 	tablename NOT LIKE 'pg_%' AND
	// 	tablename NOT LIKE 'sql_%';
	// `

	layerSQL := `
	SELECT
		Format ( 'public.%s', "table_name" ) AS ID,
		'public' AS SCHEMA,
		"table_name" AS TABLE,
		'description' AS description,
		'geom' AS geometry_column,
		4326 AS srid,
		'raster' AS geometry_type,
		'xyz' AS id_column,
		'{{zoom_level,int4,"",1},{tile_column,int4,"",2},{tile_row,int4,"",3},{tile_data,bytea,"",4}}' AS props 
	FROM
		information_schema.COLUMNS 
	WHERE
		table_schema = 'public' 
		AND COLUMN_NAME IN ( 'zoom_level', 'tile_column', 'tile_row', 'tile_data' ) 
	GROUP BY
		"table_name" 
	HAVING
		COUNT ( * ) = 4;
	`

	db, connerr := dbConnect()
	if connerr != nil {
		return nil, connerr
	}

	rows, err := db.Query(context.Background(), layerSQL)
	if err != nil {
		return nil, connerr
	}

	// Reset array of layers
	layerTables := make([]LayerRaster, 0)
	for rows.Next() {

		var (
			id, schema, table, description, geometryColumn string
			srid                                           int
			geometryType, idColumn                         string
			atts                                           pgtype.TextArray
		)

		err := rows.Scan(&id, &schema, &table, &description, &geometryColumn,
			&srid, &geometryType, &idColumn, &atts)
		if err != nil {
			return nil, err
		}

		// We use https://godoc.org/github.com/jackc/pgtype#TextArray
		// here to scan the text[][] map of property name/type
		// created in the query. It gets a little ugly demapping the
		// pgx TextArray type, but it is at least native handling of
		// the array. It's complex because of PgSQL ARRAY generality
		// really, no fault of pgx
		properties := make(map[string]RasterProperty)

		if atts.Status == pgtype.Present {
			arrLen := atts.Dimensions[0].Length
			arrStart := atts.Dimensions[0].LowerBound - 1
			elmLen := atts.Dimensions[1].Length
			for i := arrStart; i < arrLen; i++ {
				pos := i * elmLen
				elmID := atts.Elements[pos].String
				elm := RasterProperty{
					Name:        elmID,
					Type:        atts.Elements[pos+1].String,
					Description: atts.Elements[pos+2].String,
				}
				elm.order, _ = strconv.Atoi(atts.Elements[pos+3].String)
				properties[elmID] = elm
			}
		}

		// "schema.tablename" is our unique key for table layers
		lyr := LayerRaster{
			ID:             id,
			Schema:         schema,
			Table:          table,
			Description:    description,
			GeometryColumn: geometryColumn,
			Srid:           srid,
			GeometryType:   geometryType,
			IDColumn:       idColumn,
			Properties:     properties,
		}

		layerTables = append(layerTables, lyr)
	}
	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return layerTables, nil
}
