package models

import (
	"database/sql/driver"
	"fmt"
)

type Datastore struct {
	Cluster                 string  `db:"cluster" json:"-"`
	DiskId                  string  `db:"diskId" json:"diskId"`
	FreeCapacityGB          float64 `db:"freeCapacityGB" json:"freeCapacityGB"`
	HardwareAcceleratedMove bool    `db:"hardwareAcceleratedMove" json:"hardwareAcceleratedMove"`
	HostId                  string  `db:"hostId" json:"hostId"`
	Model                   string  `db:"model" json:"model"`
	ProtocolType            string  `db:"protocolType" json:"protocolType"`
	TotalCapacityGB         float64 `db:"totalCapacityGB" json:"totalCapacityGB"`
	Type                    string  `db:"type" json:"type"`
	Vendor                  string  `db:"vendor" json:"vendor"`
}

type Os struct {
	Name  string `db:"name" json:"name"`
	Count int    `db:"count" json:"count"`
}

type Host struct {
	Cluster    string `db:"cluster" json:"-"`
	CpuCores   int    `db:"cpuCores" json:"cpuCores"`
	CpuSockets int    `db:"cpuSockets" json:"cpuSockets"`
	Id         string `db:"id" json:"id"`
	MemoryMB   int    `db:"memoryMB" json:"memoryMB"`
	Model      string `db:"model" json:"model"`
	Vendor     string `db:"vendor" json:"vendor"`
}

type Network struct {
	Cluster  string `db:"cluster" json:"-"`
	Dvswitch string `db:"dvswitch" json:"dvswitch"`
	Name     string `db:"name" json:"name"`
	Type     string `db:"type" json:"type"`
	VlanId   string `db:"vlanId" json:"vlanId"`
	VmsCount int    `db:"vmsCount" json:"vmsCount"`
}

type Cluster struct {
	Name       string      `json:"name"`
	Datastores []Datastore `json:"datastores"`
	Hosts      []Host      `json:"hosts"`
	Networks   []Network   `json:"networks"`
}

type VM struct {
	ID                       string   `db:"VM ID"`                                  // vinfo
	Name                     string   `db:"VM"`                                     // vinfo
	Folder                   string   `db:"Folder ID"`                              // vinfo
	Host                     string   `db:"Host"`                                   // vinfo
	UUID                     string   `db:"SMBIOS UUID"`                            // vinfo
	Firmware                 string   `db:"Firmware"`                               // vinfo
	PowerState               string   `db:"Powerstate"`                             // vinfo
	ConnectionState          string   `db:"Connection state"`                       // vinfo
	CpuHotAddEnabled         bool     `db:"Hot Add"`                                // vcpu
	CpuHotRemoveEnabled      bool     `db:"Hot Remove"`                             // vcpu
	MemoryHotAddEnabled      bool     `db:"Hot Add"`                                // vmemory
	FaultToleranceEnabled    bool     `db:"FT State"`                               // vinfo
	CpuCount                 int32    `db:"CPUs"`                                   // vinfo
	CpuSockets               int32    `db:"Sockets"`                                // vcpu
	CoresPerSocket           int32    `db:"Cores p/s"`                              // vcpu
	MemoryMB                 int32    `db:"Memory"`                                 // vinfo
	GuestName                string   `db:"OS according to the configuration file"` // vinfo
	GuestNameFromVmwareTools string   `db:"OS according to the VMware Tools"`       // vinfo
	HostName                 string   `db:"DNS Name"`                               // vinfo
	BalloonedMemory          int32    `db:"Ballooned"`                              // vmemory
	IpAddress                string   `db:"Primary IP Address"`                     // vinfo
	StorageUsed              int32    `db:"In Use MiB"`                             // vinfo
	IsTemplate               bool     `db:"Template"`                               // vinfo
	ChangeTrackingEnabled    bool     `db:"CBT"`                                    // vinfo
	NICs                     NICs     //                                               vnetwork
	Disks                    Disks    //                                               vdisk
	Networks                 Networks `db:"network_object_id"`       // vinfo (Network #1, #2, etc.)
	DiskEnableUuid           bool     `db:"EnableUUID"`              // vinfo
	Datacenter               string   `db:"Datacenter"`              // vinfo
	Cluster                  string   `db:"Cluster"`                 // vinfo
	HWVersion                string   `db:"HW version"`              // vinfo
	TotalDiskCapacityMiB     int32    `db:"Total disk capacity MiB"` // vinfo
	ProvisionedMiB           int32    `db:"Provisioned MiB"`         // vinfo
	ResourcePool             string   `db:"Resource pool"`           // vinfo
}

// Disk represents a virtual disk from the vdisk table
type Disk struct {
	Key           string `db:"Disk Key"` // vdisk
	UnitNumber    string `db:"Unit #"`   // vdisk
	ControllerKey int32  //                      derived
	File          string `db:"Path"`         // vdisk
	Capacity      int64  `db:"Capacity MiB"` // vdisk
	Shared        bool   `db:"Sharing mode"` // vdisk
	RDM           bool   `db:"Raw"`          // vdisk
	Bus           string `db:"Shared Bus"`   // vdisk
	Mode          string `db:"Disk Mode"`    // vdisk
	Serial        string `db:"Disk UUID"`    // vdisk
	Thin          string `db:"Thin"`         // vdisk
	Controller    string `db:"Controller"`   // vdisk
	Label         string `db:"Label"`        // vdisk
	SCSIUnit      string `db:"SCSI Unit #"`  // vdisk
}

// NIC represents a network interface from the vnetwork table
type NIC struct {
	Network         string `db:"Network"`          // vnetwork
	MAC             string `db:"Mac Address"`      // vnetwork
	Label           string `db:"NIC label"`        // vnetwork
	Adapter         string `db:"Adapter"`          // vnetwork
	Switch          string `db:"Switch"`           // vnetwork
	Connected       bool   `db:"Connected"`        // vnetwork
	StartsConnected bool   `db:"Starts Connected"` // vnetwork
	Type            string `db:"Type"`             // vnetwork
	IPv4Address     string `db:"IPv4 Address"`     // vnetwork
	IPv6Address     string `db:"IPv6 Address"`     // vnetwork
}

type Concern struct {
	Id         string `json:"id" db:"Concern_ID"`
	Label      string `json:"label" db:"Label"`
	Category   string `json:"category" db:"Category"`
	Assessment string `json:"assessment" db:"Assessment"`
}

type Inventory struct {
	VcenterId string                   `json:"vcenterId"`           // unique identifier for vCenter (from "VI SDK UUID" in vInfo)
	Clusters  map[string]InventoryData `json:"clusters"`            // cluster ID -> per-cluster inventory data
	OsSummary []Os                     `json:"osSummary,omitempty"` // OS distribution summary (vcenter-level)
}

type InventoryData struct {
	Infra Infra `json:"infra"` // infrastructure data (hosts, datastores, networks)
	VMs   []VM  `json:"vms"`   // virtual machines
}

type Infra struct {
	Hosts                 []Host         `json:"hosts"`                           // ESXi hosts
	Datastores            []Datastore    `json:"datastores"`                      // datastores
	Networks              []Network      `json:"networks"`                        // networks
	HostPowerStates       map[string]int `json:"hostPowerStates,omitempty"`       // power state (green/yellow/red/gray) -> count
	TotalHosts            int            `json:"totalHosts"`                      // total number of hosts
	TotalDatacenters      int            `json:"totalDatacenters,omitempty"`      // total number of datacenters
	ClustersPerDatacenter []int          `json:"clustersPerDatacenter,omitempty"` // number of clusters per datacenter
}

// Disks is a slice of Disk that implements sql.Scanner for DuckDB LIST type
type Disks []Disk

func (d *Disks) Scan(value interface{}) error {
	if value == nil {
		*d = nil
		return nil
	}
	slice, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("Disks.Scan: expected []interface{}, got %T", value)
	}
	result := make([]Disk, 0, len(slice))
	for _, item := range slice {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		disk := Disk{
			Key:        toString(m["Key"]),
			UnitNumber: toString(m["UnitNumber"]),
			File:       toString(m["File"]),
			Capacity:   toInt64(m["Capacity"]),
			Shared:     toBool(m["Shared"]),
			RDM:        toBool(m["RDM"]),
			Bus:        toString(m["Bus"]),
			Mode:       toString(m["Mode"]),
			Serial:     toString(m["Serial"]),
			Thin:       toString(m["Thin"]),
			Controller: toString(m["Controller"]),
			Label:      toString(m["Label"]),
			SCSIUnit:   toString(m["SCSIUnit"]),
		}
		result = append(result, disk)
	}
	*d = result
	return nil
}

func (d Disks) Value() (driver.Value, error) {
	return d, nil
}

// NICs is a slice of NIC that implements sql.Scanner for DuckDB LIST type
type NICs []NIC

func (n *NICs) Scan(value interface{}) error {
	if value == nil {
		*n = nil
		return nil
	}
	slice, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("NICs.Scan: expected []interface{}, got %T", value)
	}
	result := make([]NIC, 0, len(slice))
	for _, item := range slice {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		nic := NIC{
			Network:         toString(m["Network"]),
			MAC:             toString(m["MAC"]),
			Label:           toString(m["Label"]),
			Adapter:         toString(m["Adapter"]),
			Switch:          toString(m["Switch"]),
			Connected:       toBool(m["Connected"]),
			StartsConnected: toBool(m["StartsConnected"]),
			Type:            toString(m["Type"]),
			IPv4Address:     toString(m["IPv4Address"]),
			IPv6Address:     toString(m["IPv6Address"]),
		}
		result = append(result, nic)
	}
	*n = result
	return nil
}

func (n NICs) Value() (driver.Value, error) {
	return n, nil
}

// Networks is a slice of strings that implements sql.Scanner for DuckDB LIST type
type Networks []string

func (n *Networks) Scan(value interface{}) error {
	if value == nil {
		*n = nil
		return nil
	}
	slice, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("Networks.Scan: expected []interface{}, got %T", value)
	}
	result := make([]string, 0, len(slice))
	for _, item := range slice {
		if s := toString(item); s != "" {
			result = append(result, s)
		}
	}
	*n = result
	return nil
}

func (n Networks) Value() (driver.Value, error) {
	return n, nil
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func toInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case int32:
		return int64(val)
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		var i int64
		fmt.Sscanf(val, "%d", &i)
		return i
	}
	return 0
}

func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case string:
		return val == "true" || val == "True" || val == "1" || val == "Yes"
	case int64:
		return val != 0
	case int32:
		return val != 0
	case int:
		return val != 0
	}
	return false
}
