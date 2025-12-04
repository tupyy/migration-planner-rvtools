package definitions

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
