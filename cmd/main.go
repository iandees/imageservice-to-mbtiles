package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // Register sqlite3 database driver
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/paulmach/orb/maptile/tilecover"

	"github.com/iandees/imageservice-to-mbtiles/pkg/esriservice"
)

const (
	minZoom     = maptile.Zoom(12)
	maxZoom     = maptile.Zoom(20)
	concurrency = 32
)

type imageResult struct {
	imageBytes []byte
	tile       maptile.Tile
}

type imageRequest struct {
	tile maptile.Tile
}

func main() {
	endpoint := flag.String("endpoint", "", "An ESRI REST service endpoint that ends in /MapServer or /ImageServer")
	outputFilename := flag.String("output", "", "Path to the output mbtiles")
	flag.Parse()

	ctx := context.Background()

	if endpoint == nil || *endpoint == "" {
		log.Fatalf("Must supply --endpoint")
	}

	if outputFilename == nil || *outputFilename == "" {
		log.Fatalf("Must supply --output")
	}

	esriClient := esriservice.NewClient(*endpoint)

	details, err := esriClient.GetDetails(ctx)
	if err != nil {
		log.Fatalf("Coudln't get details for endpoint: %+v", err)
	}

	input := &esriservice.ExportImageInput{
		ImageSR:     4326,
		BoundingBox: details.FullExtent,
		Size:        esriservice.RectType{Width: 512, Height: 512},
		Format:      "png",
		PixelType:   "u8",
	}
	resp, err := esriClient.ExportImage(ctx, input)
	if err != nil {
		log.Fatalf("Couldn't export image: %+v", err)
	}

	log.Printf("Extent of 4326 image: %0.5f,%0.5f,%0.5f,%0.5f", resp.Extent.XMin, resp.Extent.YMin, resp.Extent.XMax, resp.Extent.YMax)

	resultPipe := make(chan *imageResult, 1000)
	requestPipe := make(chan *imageRequest, 5000000)
	requestWG := &sync.WaitGroup{}
	writerWG := &sync.WaitGroup{}

	go func() {
		completeExtent := orb.Bound{
			Min: orb.Point{resp.Extent.XMin, resp.Extent.YMin},
			Max: orb.Point{resp.Extent.XMax, resp.Extent.YMax},
		}

		coveringTiles := tilecover.Bound(completeExtent, minZoom)
		log.Printf("Found %d tiles to fetch at z%d", len(coveringTiles), minZoom)

		for t := range coveringTiles {
			requestPipe <- &imageRequest{
				tile: t,
			}
		}
		log.Printf("Don't inserting first zoom")
	}()

	go func() {
		for range time.Tick(1 * time.Second) {
			log.Printf("Requests: %4d, Results: %4d", len(requestPipe), len(resultPipe))
		}
	}()

	for i := 0; i < concurrency; i++ {
		requestWG.Add(1)
		go func() {
			defer requestWG.Done()
			for req := range requestPipe {
				tileBounds := req.tile.Bound()
				imageBounds := esriservice.ExtentType{
					XMin:             tileBounds.Min.X(),
					YMin:             tileBounds.Min.Y(),
					XMax:             tileBounds.Max.X(),
					YMax:             tileBounds.Max.Y(),
					SpatialReference: esriservice.SpatialReferenceType{Wkid: 4326},
				}

				imageFetchContext, cancel := context.WithTimeout(ctx, 15*time.Second)
				input := &esriservice.ExportImageInput{
					ImageSR:     3857,
					BoundingBox: imageBounds,
					Size:        esriservice.RectType{Width: 256, Height: 256},
					Format:      "png",
					PixelType:   "u8",
					NoData:      []int{255},
				}
				resp, err := esriClient.ExportImage(imageFetchContext, input)
				if err != nil {
					log.Fatalf("Couldn't export image: %+v", err)
				}

				imageReq, err := http.NewRequestWithContext(imageFetchContext, "GET", resp.Href, nil)
				if err != nil {
					log.Fatalf("Couldn't build request to exported image: %+v", err)
				}

				response, err := http.DefaultClient.Do(imageReq)
				if err != nil {
					log.Fatalf("Couldn't fetch referred image: %+v", err)
				}

				imageBytes, err := ioutil.ReadAll(response.Body)
				if err != nil {
					log.Fatalf("Couldn't copy image bytes: %+v", err)
				}

				response.Body.Close()
				cancel()

				resultPipe <- &imageResult{
					imageBytes: imageBytes,
					tile:       req.tile,
				}
			}
			log.Printf("Closing request pipe")
		}()
	}

	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		dsn := fmt.Sprintf("file:%s?_journal_mode=MEMORY&_synchronous=OFF", *outputFilename)
		db, err := sql.Open("sqlite3", dsn)
		if err != nil {
			log.Fatalf("Couldn't open database: %+v", err)
		}

		if _, err := db.Exec(`
		BEGIN TRANSACTION;
		CREATE TABLE IF NOT EXISTS tiles (
			zoom_level INT NOT NULL,
			tile_column INT NOT NULL,
			tile_row INT NOT NULL,
			tile_data BLOB NOT NULL
		);
		CREATE UNIQUE INDEX IF NOT EXISTS tiles_index ON tiles (zoom_level, tile_column, tile_row);
        CREATE TABLE IF NOT EXISTS metadata (
            name TEXT,
            value TEXT
        );
        INSERT INTO metadata (name, value) VALUES
		  ('name', ?),
		  ('format', 'png'),
		  ('minzoom', ?),
		  ('maxzoom', ?),
		  ('scheme', 'tms');
		COMMIT;
	`, "kamloops", minZoom, maxZoom); err != nil {
			log.Fatalf("Couldn't create table: %+v", err)
		}

		tx, err := db.Begin()
		if err != nil {
			log.Fatalf("Couldn't create transaction: %+v", err)
		}

		tileInsertStmt, err := tx.Prepare("INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?);")
		if err != nil {
			log.Fatalf("Couldn't create insert prepared statement: %+v", err)
		}

		count := 0
		for r := range resultPipe {
			// TODO Is there a better way to find blank tiles?
			if len(r.imageBytes) == 777 || len(r.imageBytes) == 776 {
				// Don't write or recurse into the next level because this tile was completely blank
				continue
			}

			// "Invert the Y" to get to a TMS tile coordinate for mbtiles
			flippedY := (1 << r.tile.Z) - 1 - r.tile.Y

			_, err = tileInsertStmt.Exec(r.tile.Z, r.tile.X, flippedY, r.imageBytes)
			if err != nil {
				log.Fatalf("Couldn't exec insert statement: %+v", err)
			}

			// log.Printf("Wrote %d bytes to tile %d/%d/%d", len(r.imageBytes), r.tile.Z, r.tile.X, flippedY)

			count++
			if count%1000 == 0 {
				log.Printf("Committed")
				err := tx.Commit()
				if err != nil {
					log.Fatalf("Couldn't commit transaction: %+v", err)
				}

				tx, err = db.Begin()
				if err != nil {
					log.Fatalf("Couldn't create transaction: %+v", err)
				}

				tileInsertStmt, err = tx.Prepare("INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?);")
				if err != nil {
					log.Fatalf("Couldn't create insert prepared statement: %+v", err)
				}
			}

			if r.tile.Z+1 > maxZoom {
				// Don't recurse past maxZoom
				continue
			}

			for _, childTile := range r.tile.Children() {
				requestPipe <- &imageRequest{
					tile: childTile,
				}
			}
		}

		err = tileInsertStmt.Close()
		if err != nil {
			log.Fatalf("Couldn't close insert statement: %+v", err)
		}

		err = tx.Commit()
		if err != nil {
			log.Fatalf("Couldn't commit transaction: %+v", err)
		}

		err = db.Close()
		if err != nil {
			log.Fatalf("Couldn't close database: %+v", err)
		}
	}()

	requestWG.Wait()
	close(resultPipe)
	writerWG.Wait()

	log.Printf("Done")
}
