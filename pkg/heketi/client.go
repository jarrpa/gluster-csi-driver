package heketi

import (
	"fmt"
	"strings"

	cli "github.com/gluster/gluster-csi-driver/pkg/client"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	hcli "github.com/heketi/heketi/client/api/go-client"
	hapi "github.com/heketi/heketi/pkg/glusterfs/api"
)

const (
	heketiDefaultBlock              = false
	heketiDefaultReplica            = 3
	heketiDefaultDisperseData       = 3
	heketiDefaultDisperseRedundancy = 2
	heketiDefaultGid                = 0
	heketiDefaultSnapshot           = true
	heketiDefaultSnapFactor         = 1.0
)

type heketiClient struct {
	client     *hcli.Client
	driverName string
	url        string
	username   string
	password   string
}

// SetDefaultClient sets the default client as a heketiClient
func SetDefaultClient(driverName string) {
	cli.GlusterClientCache.DefaultClient = heketiClient{
		driverName: driverName,
	}
}

// GetClusterNodes retrieves a list of cluster peer nodes
func (gc heketiClient) GetClusterNodes(volumeID string) ([]string, error) {
	glusterServers := []string{}

	clr, err := gc.client.ClusterList()
	if err != nil {
		return nil, fmt.Errorf("failed to list heketi clusters: %v", err)
	}
	cluster := clr.Clusters[0]
	clusterInfo, err := gc.client.ClusterInfo(cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster %s details: %v", cluster, err)
	}

	for _, node := range clusterInfo.Nodes {
		nodeInfo, err := gc.client.NodeInfo(node)
		if err != nil {
			return nil, fmt.Errorf("failed to get node %s info: %v", node, err)
		}
		nodeAddr := strings.Join(nodeInfo.NodeAddRequest.Hostnames.Storage, "")
		glusterServers = append(glusterServers, nodeAddr)
	}

	return glusterServers, nil
}

// CheckExistingVolume checks whether a given volume already exists
func (gc heketiClient) CheckExistingVolume(volumeID string, volSizeBytes int64) error {
	var vol *csi.Volume
	vols, err := gc.ListVolumes()
	if err != nil {
		return fmt.Errorf("error listing volumes: %v", err)
	}

	for _, volEnt := range vols {
		if volEnt.Id == volumeID {
			vol = volEnt
		}
	}
	if vol == nil {
		return cli.ErrNotFound("volume", volumeID)
	}

	if volSizeBytes > 0 && cli.RoundUpToGiB(vol.CapacityBytes) != cli.RoundUpToGiB(volSizeBytes) {
		return fmt.Errorf("volume %s already exists with different size: %d GiB", volumeID, cli.RoundUpToGiB(vol.CapacityBytes))
	}

	return nil
}

// CreateVolume creates a new volume
func (gc heketiClient) CreateVolume(volumeName string, volSizeBytes int64, params map[string]string) error {
	durabilityInfo := hapi.VolumeDurabilityInfo{Type: hapi.DurabilityReplicate, Replicate: hapi.ReplicaDurability{Replica: heketiDefaultReplica}}
	volumeType := params["glustervolumetype"]
	if len(volumeType) != 0 {
		volumeTypeList := strings.Split(volumeType, ":")

		switch volumeTypeList[0] {
		case "replicate":
			if len(volumeTypeList) >= 2 {
				durabilityInfo.Replicate.Replica = cli.ParseIntWithDefault(volumeTypeList[1], heketiDefaultReplica)
			}
		case "disperse":
			if len(volumeTypeList) >= 3 {
				data := cli.ParseIntWithDefault(volumeTypeList[1], heketiDefaultDisperseData)
				redundancy := cli.ParseIntWithDefault(volumeTypeList[2], heketiDefaultDisperseRedundancy)
				durabilityInfo = hapi.VolumeDurabilityInfo{Type: hapi.DurabilityEC, Disperse: hapi.DisperseDurability{Data: data, Redundancy: redundancy}}
			} else {
				return fmt.Errorf("volume type 'disperse' must have format: 'disperse:<data>:<redundancy>'")
			}
		case "none":
			durabilityInfo = hapi.VolumeDurabilityInfo{Type: hapi.DurabilityDistributeOnly}
		default:
			return fmt.Errorf("invalid volume type %s", volumeType)
		}
	}

	volumeReq := &hapi.VolumeCreateRequest{
		Size:                 int(cli.RoundUpToGiB(volSizeBytes)),
		Name:                 volumeName,
		Durability:           durabilityInfo,
		Gid:                  int64(cli.ParseIntWithDefault(params["glustergid"], heketiDefaultGid)),
		GlusterVolumeOptions: strings.Split(params["glustervolumeoptions"], ","),
		Block:                cli.ParseBoolWithDefault(params["glusterblockhost"], heketiDefaultBlock),
		Snapshot: struct {
			Enable bool    `json:"enable"`
			Factor float32 `json:"factor"`
		}{
			Enable: cli.ParseBoolWithDefault(params["glustersnapshot"], heketiDefaultSnapshot),
			Factor: cli.ParseFloatWithDefault(params["glustersnapfactor"], heketiDefaultSnapFactor),
		},
	}

	_, err := gc.client.VolumeCreate(volumeReq)
	if err != nil {
		return fmt.Errorf("failed to create volume %s: %v", volumeName, err)
	}

	return nil
}

// DeleteVolume deletes a volume
func (gc heketiClient) DeleteVolume(volumeID string) error {
	var vol *csi.Volume
	vols, err := gc.ListVolumes()
	if err != nil {
		return fmt.Errorf("error listing volumes: %v", err)
	}

	for _, volEnt := range vols {
		if volEnt.Id == volumeID {
			vol = volEnt
		}
	}
	if vol == nil {
		return cli.ErrNotFound("volume", volumeID)
	}

	err = gc.client.VolumeDelete(vol.Attributes["glustervolheketiid"])
	if err != nil {
		if err.Error() == "Invalid path or request" {
			return cli.ErrNotFound("volume", volumeID)
		}
		return err
	}

	return nil
}

// ListVolumes lists all volumes in the cluster
func (gc heketiClient) ListVolumes() ([]*csi.Volume, error) {
	volumes := []*csi.Volume{}

	vols, err := gc.client.VolumeList()
	if err != nil {
		return nil, err
	}
	for _, volID := range vols.Volumes {
		vol, err := gc.client.VolumeInfo(volID)
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, &csi.Volume{
			Id:            vol.VolumeInfo.VolumeCreateRequest.Name,
			CapacityBytes: (int64(vol.VolumeCreateRequest.Size)) * cli.GiB,
			Attributes: map[string]string{
				"glustervolheketiid": vol.VolumeInfo.Id,
			},
		})
	}

	return volumes, nil
}

// CheckExistingSnapshot checks if a snapshot exists in the TSP
func (gc heketiClient) CheckExistingSnapshot(snapName, volName string) (*csi.Snapshot, error) {
	return nil, nil
}

// CreateSnapshot creates a snapshot of an existing volume
func (gc heketiClient) CreateSnapshot(snapName, srcVol string) (*csi.Snapshot, error) {
	return nil, nil
}

// CloneSnapshot creates a clone of a snapshot
func (gc heketiClient) CloneSnapshot(snapName, volName string) error {
	return nil
}

// DeleteSnapshot deletes a snapshot
func (gc heketiClient) DeleteSnapshot(snapName string) error {
	return nil
}

// ListSnapshots lists all snapshots
func (gc heketiClient) ListSnapshots(snapName, srcVol string) ([]*csi.Snapshot, error) {
	return nil, nil
}

// Update returns a new heketiClient (GlusterClient) with updated parameters.
// If a REST client has not yet been established, it is created and tested.
func (gc heketiClient) Update(params map[string]string) (cli.GlusterClient, error) {
	client := cli.GlusterClientCache.DefaultClient.(heketiClient)

	if len(params["glusteruser"]) == 0 {
		params["glusteruser"] = "admin"
	}

	client.url = cli.SetStringIfEmpty(gc.url, params["glusterurl"])
	client.username = cli.SetStringIfEmpty(gc.username, params["glusteruser"])
	client.password = cli.SetStringIfEmpty(gc.password, params["glustersecret"])

	if gc.client == nil {
		heketi := hcli.NewClient(client.url, client.username, client.password)
		err := heketi.Hello()
		if err != nil {
			return client, fmt.Errorf("error finding Heketi server at %s: %v", client.url, err)
		}

		gc.client = heketi
	}

	client.client = gc.client

	return client, nil
}
