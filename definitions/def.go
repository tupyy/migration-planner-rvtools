package definitions

var (
	Sheets = []string{
		"vInfo",
		"vCPU",
		"vMemory",
		"vDisk",
		"vPartition",
		"vHost",
		"vDatastore",
		"vNetwork",
		"vCluster",
		"dvSwitch",
		"vHBA",
		"dvPort",
	}

	CreateTableStmt = `CREATE TABLE %s AS SELECT * FROM read_xlsx("%s",sheet="%s",all_varchar=true);`

	SelectOsStmt = `SELECT
		"OS according to the VMware Tools" as "name",
		COUNT("OS according to the VMware Tools") as "count" from vinfo
		WHERE "OS according to the VMware Tools" IS NOT NULL
		GROUP BY "OS according to the VMware Tools"
		ORDER BY "OS according to the VMware Tools";
`

	SelectDatastoreStmt = `
WITH expanded AS (
       SELECT
           *,
           trim(unnest(string_split(Hosts, ','))) AS ip
       FROM vdatastore
       WHERE "Cluster name" IS NOT NULL
   )
   SELECT
       e."Cluster name" as "cluster",
       e."Name" as "diskId",
       (e."Free MiB"::double / 1024)::integer as "freeCapacityGB",
       (e."MHA" = 'True') as "hardwareAcceleratedMove",
       COALESCE(string_agg(h."Object ID", ', '), 'N/A') AS "hostId",
       'N/A' as "model",
       'N/A' as "protocolType",
       (e."Capacity MiB"::double / 1024)::integer as "totalCapacityGB",
       COALESCE(e."Type", 'N/A') as "type",
       'N/A' as "vendor"
   FROM expanded e
   LEFT JOIN vhost h ON h.Host = e.ip
   GROUP BY ALL;
`

	SelectDatastoreSimpleStmt = `
   SELECT
       "Cluster name" as "cluster",
       "Name" as "diskId",
       ("Free MiB"::double / 1024)::integer as "freeCapacityGB",
       ("MHA" = 'True') as "hardwareAcceleratedMove",
       'N/A' AS "hostId",
       'N/A' as "model",
       'N/A' as "protocolType",
       ("Capacity MiB"::double / 1024)::integer as "totalCapacityGB",
       COALESCE("Type", 'N/A') as "type",
       'N/A' as "vendor"
   FROM vdatastore
   WHERE "Cluster name" IS NOT NULL;
`

	SelectHostStmt = `
   SELECT
       "Cluster" as "cluster",
       "# Cores"::integer as "cpuCores",
       "# CPU"::integer as "cpuSockets",
       "Object ID" as "id",
       "# Memory"::integer as "memoryMB",
       COALESCE("Model", 'N/A') as "model",
       COALESCE("Vendor", 'N/A') as "vendor"
   FROM vhost
   WHERE "Cluster" IS NOT NULL;
`

	SelectNetworkStmt = `
   SELECT
       n."Cluster" as "cluster",
       COALESCE(n."Switch", '') as "dvswitch",
       n."Network" as "name",
       'distributed' as "type",
       COALESCE(p."VLAN", '') as "vlanId",
       COUNT(*)::integer as "vmsCount"
   FROM vnetwork n
   LEFT JOIN dvport p ON n."Network" = p."Port"
   WHERE n."Cluster" IS NOT NULL
   GROUP BY n."Cluster", n."Switch", n."Network", p."VLAN";
`

	SelectNetworkSimpleStmt = `
   SELECT
       "Cluster" as "cluster",
       COALESCE("Switch", '') as "dvswitch",
       "Network" as "name",
       'distributed' as "type",
       '' as "vlanId",
       COUNT(*)::integer as "vmsCount"
   FROM vnetwork
   WHERE "Cluster" IS NOT NULL
   GROUP BY "Cluster", "Switch", "Network";
`
)
