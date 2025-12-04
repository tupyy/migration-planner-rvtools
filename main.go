package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/georgysavva/scany/v2/sqlscan"

	"github.com/tupyy/rvtools/definitions"
)

var (
	excelFile string
	dbPath    string
)

func main() {
	flag.StringVar(&excelFile, "excel-file", "", "path of excel file")
	flag.StringVar(&dbPath, "db-path", "", "Path to db file")
	flag.Parse()

	c, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		log.Fatalf("could not initialize new connector: %s", err.Error())
	}
	defer c.Close()

	db := sql.OpenDB(c)
	defer db.Close()

	if err := loadExtentions(db); err != nil {
		log.Printf("loading extentions: %v", err)
		os.Exit(1)
	}

	count := readExcel(db, excelFile)
	if count == 0 {
		log.Panicf("reading excel: %s", err)
		os.Exit(1)
	}

	ctx := context.Background()

	datastores, err := readDatastore(ctx, db)
	if err != nil {
		log.Printf("reading datastores: %v", err)
	}
	hosts, err := readHosts(ctx, db)
	if err != nil {
		log.Printf("reading hosts: %v", err)
	}
	networks, err := readNetworks(ctx, db)
	if err != nil {
		log.Printf("reading networks: %v", err)
	}
	clusters := groupByCluster(datastores, hosts, networks)

	data, _ := json.MarshalIndent(clusters, "", "  ")
	fmt.Println(string(data))

	osList, _ := readOs(ctx, db)
	osData, _ := json.MarshalIndent(osList, "", "  ")
	fmt.Println(string(osData))

	log.Printf("number of sheets created: %d", count)
}

func loadExtentions(db *sql.DB) error {
	_, err := db.Exec("install excel;load excel;")
	return err
}

func tableExists(db *sql.DB, table string) bool {
	var count int
	err := db.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name = ?", table).Scan(&count)
	return err == nil && count > 0
}

func readExcel(db *sql.DB, excelFile string) int {
	countSheet := 0
	for _, s := range definitions.Sheets {
		if _, err := db.Exec(fmt.Sprintf(definitions.CreateTableStmt, strings.ToLower(s), excelFile, s)); err != nil {
			log.Printf("failed to create sheet %s: %v", s, err)
			continue
		}
		countSheet++
	}
	return countSheet
}

func readDatastore(ctx context.Context, db *sql.DB) ([]definitions.Datastore, error) {
	query := definitions.SelectDatastoreStmt
	if !tableExists(db, "vhost") {
		query = definitions.SelectDatastoreSimpleStmt
	}

	var results []definitions.Datastore
	if err := sqlscan.Select(ctx, db, &results, query); err != nil {
		return nil, fmt.Errorf("scanning datastores: %w", err)
	}
	return results, nil
}

func readOs(ctx context.Context, db *sql.DB) ([]definitions.Os, error) {
	var results []definitions.Os
	if err := sqlscan.Select(ctx, db, &results, definitions.SelectOsStmt); err != nil {
		return nil, fmt.Errorf("scanning os: %w", err)
	}
	return results, nil
}

func readHosts(ctx context.Context, db *sql.DB) ([]definitions.Host, error) {
	var results []definitions.Host
	if err := sqlscan.Select(ctx, db, &results, definitions.SelectHostStmt); err != nil {
		return nil, fmt.Errorf("scanning hosts: %w", err)
	}
	return results, nil
}

func readNetworks(ctx context.Context, db *sql.DB) ([]definitions.Network, error) {
	if !tableExists(db, "vnetwork") {
		return nil, nil
	}
	query := definitions.SelectNetworkStmt
	if !tableExists(db, "dvport") {
		query = definitions.SelectNetworkSimpleStmt
	}
	var results []definitions.Network
	if err := sqlscan.Select(ctx, db, &results, query); err != nil {
		return nil, fmt.Errorf("scanning networks: %w", err)
	}
	return results, nil
}

func groupByCluster(datastores []definitions.Datastore, hosts []definitions.Host, networks []definitions.Network) []definitions.Cluster {
	clusterMap := make(map[string]*definitions.Cluster)

	for _, ds := range datastores {
		if _, ok := clusterMap[ds.Cluster]; !ok {
			clusterMap[ds.Cluster] = &definitions.Cluster{Name: ds.Cluster}
		}
		clusterMap[ds.Cluster].Datastores = append(clusterMap[ds.Cluster].Datastores, ds)
	}

	for _, h := range hosts {
		if _, ok := clusterMap[h.Cluster]; !ok {
			clusterMap[h.Cluster] = &definitions.Cluster{Name: h.Cluster}
		}
		clusterMap[h.Cluster].Hosts = append(clusterMap[h.Cluster].Hosts, h)
	}

	for _, n := range networks {
		if _, ok := clusterMap[n.Cluster]; !ok {
			clusterMap[n.Cluster] = &definitions.Cluster{Name: n.Cluster}
		}
		clusterMap[n.Cluster].Networks = append(clusterMap[n.Cluster].Networks, n)
	}

	clusters := make([]definitions.Cluster, 0, len(clusterMap))
	for _, c := range clusterMap {
		clusters = append(clusters, *c)
	}
	return clusters
}
