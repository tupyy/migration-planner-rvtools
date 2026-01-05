-- Test fixtures for RVTools-shaped tables
-- Uses the fixed schema from create_schema.go.tmpl

-- vinfo: VM information
CREATE TABLE vinfo (
    "VM ID" VARCHAR,
    "VM" VARCHAR,
    "Folder ID" VARCHAR,
    "Folder" VARCHAR,
    "Host" VARCHAR,
    "SMBIOS UUID" VARCHAR,
    "VM UUID" VARCHAR,
    "Firmware" VARCHAR,
    "Powerstate" VARCHAR,
    "Connection state" VARCHAR,
    "FT State" VARCHAR,
    "CPUs" INTEGER DEFAULT 0,
    "Memory" INTEGER DEFAULT 0,
    "OS according to the configuration file" VARCHAR,
    "OS according to the VMware Tools" VARCHAR,
    "DNS Name" VARCHAR,
    "Primary IP Address" VARCHAR,
    "In Use MiB" INTEGER DEFAULT 0,
    "Template" BOOLEAN DEFAULT false,
    "CBT" BOOLEAN DEFAULT false,
    "EnableUUID" BOOLEAN DEFAULT false,
    "Datacenter" VARCHAR,
    "Cluster" VARCHAR,
    "HW version" VARCHAR,
    "Total disk capacity MiB" INTEGER DEFAULT 0,
    "Provisioned MiB" INTEGER DEFAULT 0,
    "Resource pool" VARCHAR,
    "VI SDK UUID" VARCHAR,
    "Network #1" VARCHAR,
    "Network #2" VARCHAR,
    "Network #3" VARCHAR,
    "Network #4" VARCHAR,
    "Network #5" VARCHAR,
    "Network #6" VARCHAR,
    "Network #7" VARCHAR,
    "Network #8" VARCHAR,
    "Network #9" VARCHAR,
    "Network #10" VARCHAR,
    "Network #11" VARCHAR,
    "Network #12" VARCHAR,
    "Network #13" VARCHAR,
    "Network #14" VARCHAR,
    "Network #15" VARCHAR,
    "Network #16" VARCHAR,
    "Network #17" VARCHAR,
    "Network #18" VARCHAR,
    "Network #19" VARCHAR,
    "Network #20" VARCHAR,
    "Network #21" VARCHAR,
    "Network #22" VARCHAR,
    "Network #23" VARCHAR,
    "Network #24" VARCHAR,
    "Network #25" VARCHAR
);

INSERT INTO vinfo ("VM ID", "VM", "Folder ID", "Folder", "Host", "SMBIOS UUID", "VM UUID", "Firmware", "Powerstate", "Connection state", "FT State", "CPUs", "Memory", "OS according to the configuration file", "OS according to the VMware Tools", "DNS Name", "Primary IP Address", "In Use MiB", "Template", "CBT", "EnableUUID", "Datacenter", "Cluster", "HW version", "Total disk capacity MiB", "Provisioned MiB", "Resource pool", "VI SDK UUID", "Network #1", "Network #2") VALUES
('vm-001', 'test-vm-1', 'folder-1', 'folder-1', 'host-001', 'uuid-001', 'uuid-001', 'bios', 'poweredOn', 'connected', 'Not protected', 4, 8192, 'Red Hat Enterprise Linux 8', 'RHEL 8.5', 'testvm1.example.com', '192.168.1.10', 50000, false, true, true, 'TestDC', 'TestCluster', 'vmx-19', 102400, 204800, 'Resources', 'vcenter-uuid-001', 'network-001', 'network-002'),
('vm-002', 'test-vm-2', 'folder-1', 'folder-1', 'host-001', 'uuid-002', 'uuid-002', 'efi', 'poweredOff', 'connected', 'Not protected', 2, 4096, 'Microsoft Windows Server 2019', 'Windows 2019', 'testvm2.example.com', '192.168.1.11', 30000, false, false, false, 'TestDC', 'TestCluster', 'vmx-17', 51200, 102400, 'Resources', 'vcenter-uuid-001', 'network-001', NULL),
('vm-003', 'template-vm', 'folder-2', 'folder-2', 'host-002', 'uuid-003', 'uuid-003', 'bios', 'poweredOff', 'connected', 'Not protected', 1, 2048, 'Ubuntu 20.04', 'Ubuntu', '', '', 10000, true, false, false, 'TestDC', 'TestCluster', 'vmx-15', 20480, 40960, 'Resources', 'vcenter-uuid-001', NULL, NULL);

-- vcpu: CPU details
CREATE TABLE vcpu (
    "VM ID" VARCHAR,
    "Hot Add" BOOLEAN DEFAULT false,
    "Hot Remove" BOOLEAN DEFAULT false,
    "Sockets" INTEGER DEFAULT 0,
    "Cores p/s" INTEGER DEFAULT 0
);

INSERT INTO vcpu VALUES
('vm-001', true, false, 2, 2),
('vm-002', false, false, 1, 2),
('vm-003', false, false, 1, 1);

-- vmemory: Memory details
CREATE TABLE vmemory (
    "VM ID" VARCHAR,
    "Hot Add" BOOLEAN DEFAULT false,
    "Ballooned" INTEGER DEFAULT 0
);

INSERT INTO vmemory VALUES
('vm-001', true, 0),
('vm-002', false, 512),
('vm-003', false, 0);

-- vdisk: Disk details
CREATE TABLE vdisk (
    "VM ID" VARCHAR,
    "Disk Key" VARCHAR,
    "Unit #" VARCHAR,
    "Path" VARCHAR,
    "Disk Path" VARCHAR,
    "Capacity MiB" BIGINT DEFAULT 0,
    "Sharing mode" BOOLEAN DEFAULT false,
    "Raw" BOOLEAN DEFAULT false,
    "Shared Bus" VARCHAR,
    "Disk Mode" VARCHAR,
    "Disk UUID" VARCHAR,
    "Thin" BOOLEAN DEFAULT false,
    "Controller" VARCHAR,
    "Label" VARCHAR,
    "SCSI Unit #" VARCHAR
);

INSERT INTO vdisk VALUES
('vm-001', '2000', '0', '[datastore1] test-vm-1/disk1.vmdk', '[datastore1] test-vm-1/disk1.vmdk', 51200, false, false, 'scsi', 'persistent', 'disk-uuid-001', true, 'SCSI controller 0', 'Hard disk 1', '0'),
('vm-001', '2001', '1', '[datastore1] test-vm-1/disk2.vmdk', '[datastore1] test-vm-1/disk2.vmdk', 102400, false, false, 'scsi', 'persistent', 'disk-uuid-002', false, 'SCSI controller 0', 'Hard disk 2', '1'),
('vm-002', '2000', '0', '[datastore2] test-vm-2/disk1.vmdk', '[datastore2] test-vm-2/disk1.vmdk', 51200, false, false, 'scsi', 'persistent', 'disk-uuid-003', true, 'SCSI controller 0', 'Hard disk 1', '0');

-- vnetwork: Network interface details
CREATE TABLE vnetwork (
    "VM ID" VARCHAR,
    "Network" VARCHAR,
    "Mac Address" VARCHAR,
    "NIC label" VARCHAR,
    "Adapter" VARCHAR,
    "Switch" VARCHAR,
    "Connected" BOOLEAN DEFAULT false,
    "Starts Connected" BOOLEAN DEFAULT false,
    "Type" VARCHAR,
    "IPv4 Address" VARCHAR,
    "IPv6 Address" VARCHAR,
    "Cluster" VARCHAR
);

INSERT INTO vnetwork VALUES
('vm-001', 'VM Network', '00:50:56:aa:bb:01', 'Network adapter 1', 'vmxnet3', 'dvs-001', true, true, 'distributed', '192.168.1.10', '', 'TestCluster'),
('vm-001', 'Management', '00:50:56:aa:bb:02', 'Network adapter 2', 'vmxnet3', 'dvs-001', true, true, 'distributed', '10.0.0.10', '', 'TestCluster'),
('vm-002', 'VM Network', '00:50:56:aa:bb:03', 'Network adapter 1', 'e1000', '', true, true, 'standard', '192.168.1.11', '', 'TestCluster');

-- vhost: Host details
CREATE TABLE vhost (
    "Cluster" VARCHAR,
    "# Cores" INTEGER DEFAULT 0,
    "# CPU" INTEGER DEFAULT 0,
    "Object ID" VARCHAR,
    "# Memory" INTEGER DEFAULT 0,
    "Model" VARCHAR,
    "Vendor" VARCHAR,
    "Host" VARCHAR
);

INSERT INTO vhost VALUES
('TestCluster', 16, 2, 'host-001', 131072, 'ProLiant DL380 Gen10', 'HPE', 'host-001'),
('TestCluster', 24, 2, 'host-002', 262144, 'PowerEdge R740', 'Dell', 'host-002');

-- vdatastore: Datastore details
CREATE TABLE vdatastore (
    "Hosts" VARCHAR,
    "Address" VARCHAR,
    "Name" VARCHAR,
    "Free MiB" DOUBLE DEFAULT 0.0,
    "MHA" BOOLEAN DEFAULT false,
    "Capacity MiB" DOUBLE DEFAULT 0.0,
    "Type" VARCHAR
);

INSERT INTO vdatastore VALUES
('host-001,host-002', 'naa.001', 'datastore1', 512000, true, 1048576, 'VMFS'),
('host-001', 'naa.002', 'datastore2', 256000, false, 524288, 'NFS');

-- dvport: Distributed virtual port
CREATE TABLE dvport (
    "Port" VARCHAR,
    "VLAN" VARCHAR
);

INSERT INTO dvport VALUES
('VM Network', '100'),
('Management', '200');

-- vhba: HBA details
CREATE TABLE vhba (
    "Device" VARCHAR,
    "Type" VARCHAR
);

INSERT INTO vhba VALUES
('vmhba0', 'iSCSI'),
('vmhba1', 'FibreChannel');

-- concerns: VM validation concerns
CREATE TABLE concerns (
    "VM_ID" VARCHAR,
    "Concern_ID" VARCHAR,
    "Label" VARCHAR,
    "Category" VARCHAR,
    "Assessment" VARCHAR
);
