package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"go.uber.org/zap"

	parser "github.com/kubev2v/migration-planner/pkg/duckdb_parser"
)

var (
	excelFile       string
	sqliteFile      string
	dbPath          string
	isTimingEnabled bool
	debug           bool
)

func main() {
	flag.StringVar(&excelFile, "excel-file", "", "path of RVTools excel file")
	flag.StringVar(&sqliteFile, "sqlite-file", "", "path of forklift sqlite file")
	flag.StringVar(&dbPath, "db-path", "", "Path to db file")
	flag.BoolVar(&isTimingEnabled, "enable-timing", false, "enable timing the parsing op")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.Parse()

	// Initialize zap logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	if excelFile == "" && sqliteFile == "" {
		log.Fatal("either -excel-file or -sqlite-file must be provided")
	}
	if excelFile != "" && sqliteFile != "" {
		log.Fatal("only one of -excel-file or -sqlite-file can be provided")
	}

	now := time.Now()

	c, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		log.Fatalf("could not initialize new connector: %s", err.Error())
	}
	defer c.Close()

	db := sql.OpenDB(c)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := loadExtensions(db); err != nil {
		log.Printf("loading extensions: %v", err)
		os.Exit(1)
	}

	p := parser.New(db, nil)
	if err := p.Init(); err != nil {
		log.Fatalf("failed to initialize parser: %v", err)
	}

	if excelFile != "" {
		p.IngestRvTools(ctx, excelFile)
	} else {
		p.IngestSqlite(ctx, sqliteFile)
	}

	if isTimingEnabled {
		fmt.Printf("parsing time: %s\n", time.Since(now))
	}

	vms, err := p.VMs(context.TODO(), parser.Filters{}, parser.Options{})
	if err != nil {
		log.Fatalf("failed to get vms: %v", err)
	}
	data, _ := json.Marshal(vms)
	fmt.Println(string(data))
}

func loadExtensions(db *sql.DB) error {
	_, err := db.Exec("install excel;load excel;")
	return err
}
