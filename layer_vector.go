package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	// Database

	"github.com/jackc/pgtype"

	// Logging

	// Configuration
	"github.com/spf13/viper"
)

// LayerVector provides metadata about the table layer
type LayerVector struct {
	ID             string
	Schema         string
	Table          string
	Description    string
	Properties     map[string]VectorProperty
	GeometryType   string
	IDColumn       string
	GeometryColumn string
	Srid           int
}

// VectorProperty provides metadata about a single property field,
// features in a table layer may have multiple such fields
type VectorProperty struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	order       int
}

// VectorDetailJSON gives the output structure for the table layer.
type VectorDetailJSON struct {
	ID           string           `json:"id"`
	Schema       string           `json:"schema"`
	Name         string           `json:"name"`
	Description  string           `json:"description,omitempty"`
	Properties   []VectorProperty `json:"properties,omitempty"`
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
func (lyr LayerVector) GetType() LayerType {
	return LayerTypeVector
}

// GetID returns the complete ID (schema.name) by which to reference a given layer
func (lyr LayerVector) GetID() string {
	return lyr.ID
}

// GetDescription returns the text description for a layer
// or an empty string if no description is set
func (lyr LayerVector) GetDescription() string {
	return lyr.Description
}

// GetName returns just the name of a given layer
func (lyr LayerVector) GetName() string {
	return lyr.Table
}

// GetSchema returns just the schema for a given layer
func (lyr LayerVector) GetSchema() string {
	return lyr.Schema
}

// WriteLayerJSON outputs parameters and optional arguments for the table layer
func (lyr LayerVector) WriteLayerJSON(w http.ResponseWriter, req *http.Request) error {
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
func (lyr LayerVector) GetTileRequest(tile Tile, r *http.Request) TileRequest {

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

func (lyr *LayerVector) getRasterDetailJSON(req *http.Request) (VectorDetailJSON, error) {
	td := VectorDetailJSON{
		ID:           lyr.ID,
		Schema:       lyr.Schema,
		Name:         lyr.Table,
		Description:  lyr.Description,
		GeometryType: lyr.GeometryType,
		MinZoom:      viper.GetInt("DefaultMinZoom"),
		MaxZoom:      viper.GetInt("DefaultMaxZoom"),
	}
	// TileURL is relative to server base
	td.TileURL = fmt.Sprintf("%s/%s/{z}/{x}/{y}.pbf", serverURLBase(req), url.PathEscape(lyr.ID))

	// Want to add the properties to the Json representation
	// in table order, which is fiddly
	tmpMap := make(map[int]VectorProperty)
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
func (lyr *LayerVector) GetBoundsExact() (Bounds, error) {
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
func (lyr *LayerVector) GetBounds() (Bounds, error) {
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

func getVectorLayers() ([]LayerVector, error) {

	layerSQL := `
	SELECT
		Format ( 'vector.%s', "table_name" ) AS ID,
		'vector' AS SCHEMA,
		"table_name" AS TABLE,
		'description' AS description,
		'geom' AS geometry_column,
		4326 AS srid,
		'vector' AS geometry_type,
		'xyz' AS id_column,
		'{{zoom_level,int4,"",1},{tile_column,int4,"",2},{tile_row,int4,"",3},{tile_data,bytea,"",4}}' AS props 
	FROM
		information_schema.COLUMNS 
	WHERE
		table_schema = 'vector' 
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
	layerVectors := make([]LayerVector, 0)
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
		properties := make(map[string]VectorProperty)

		if atts.Status == pgtype.Present {
			arrLen := atts.Dimensions[0].Length
			arrStart := atts.Dimensions[0].LowerBound - 1
			elmLen := atts.Dimensions[1].Length
			for i := arrStart; i < arrLen; i++ {
				pos := i * elmLen
				elmID := atts.Elements[pos].String
				elm := VectorProperty{
					Name:        elmID,
					Type:        atts.Elements[pos+1].String,
					Description: atts.Elements[pos+2].String,
				}
				elm.order, _ = strconv.Atoi(atts.Elements[pos+3].String)
				properties[elmID] = elm
			}
		}

		// "schema.tablename" is our unique key for table layers
		lyr := LayerVector{
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

		layerVectors = append(layerVectors, lyr)
	}
	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return layerVectors, nil
}
