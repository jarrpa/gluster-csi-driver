package glusterfs

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/gluster/glusterd2/pkg/api"
	"github.com/gluster/glusterd2/pkg/restclient"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	gd2DefaultInsecure = false
)

// GlusterClient holds a GlusterFS REST client and related information
type GlusterClient struct {
	url          string
	username     string
	password     string
	client       *restclient.Client
	cacert       string
	insecure     string
	insecureBool bool
}

// GlusterClients is a map of maps of GlusterClient structs. The first map is
// indexed by the server/url the GlusterClient connects to. The second map is
// indexed by the username used on that server.
type GlusterClients map[string]map[string]*GlusterClient

var (
	glusterClientCache    = make(GlusterClients)
	glusterClientCacheMtx sync.Mutex
)

// SetPointerIfEmpty returns a new parameter if the old parameter is empty
func SetPointerIfEmpty(old, new interface{}) interface{} {
	if old == nil {
		return new
	}
	return old
}

// SetStringIfEmpty returns a new parameter if the old parameter is empty
func SetStringIfEmpty(old, new string) string {
	if len(old) == 0 {
		return new
	}
	return old
}

func (gc *GlusterClient) setInsecure(new string) {
	gc.insecure = SetStringIfEmpty(gc.insecure, new)
	insecureBool, err := strconv.ParseBool(gc.insecure)
	if err != nil {
		glog.Errorf("Bad value [%s] for glusterd2insecure, using default [%s]", gc.insecure, gd2DefaultInsecure)
		gc.insecure = strconv.FormatBool(gd2DefaultInsecure)
		insecureBool = gd2DefaultInsecure
	}
	gc.insecureBool = insecureBool
}

// GetClusterNodes returns all nodes in the TSP
func (gc *GlusterClient) GetClusterNodes() (string, []string, error) {
	glusterServer := ""
	bkpservers := []string{}

	peers, err := gc.client.Peers()
	if err != nil {
		return "", nil, err
	}

	for i, p := range peers {
		if i == 0 {
			for _, a := range p.PeerAddresses {
				ip := strings.Split(a, ":")
				glusterServer = ip[0]
			}

			continue
		}
		for _, a := range p.PeerAddresses {
			ip := strings.Split(a, ":")
			bkpservers = append(bkpservers, ip[0])
		}

	}

	glog.V(2).Infof("Gluster server and Backup servers [%+v,%+v]", glusterServer, bkpservers)

	return glusterServer, bkpservers, nil
}

// CheckExistingVolume checks if a volume exists in the TSP
func (gc *GlusterClient) CheckExistingVolume(volumeID string, volSizeMB uint64) error {
	vol, err := gc.client.Volumes(volumeID)
	if err != nil {
		errResp := gc.client.LastErrorResponse()
		//errResp will be nil in case of No route to host error
		if errResp != nil && errResp.StatusCode == http.StatusNotFound {
			return errVolumeNotFound
		}
		return err
	}

	volInfo := vol[0]
	// Do the owner validation
	if glusterAnnVal, found := volInfo.Metadata[volumeOwnerAnn]; !found || (found && glusterAnnVal != glusterfsCSIDriverName) {
		return fmt.Errorf("volume %s is not owned by %s", volInfo.Name, glusterfsCSIDriverName)
	}

	// Check requested capacity is the same as existing capacity
	if volInfo.Capacity != volSizeMB {
		return fmt.Errorf("volume %s already exists with different size: %d", volInfo.Name, volInfo.Capacity)
	}

	// If volume not started, start the volume
	if volInfo.State != api.VolStarted {
		err := gc.client.VolumeStart(volInfo.Name, true)
		if err != nil {
			return fmt.Errorf("failed to start volume %s: %v", volInfo.Name, err)
		}
	}

	glog.Infof("requested volume %s already exists in the gluster cluster", volumeID)

	return nil
}

// CheckExistingSnapshot checks if a snapshot exists in the TSP
func (gc *GlusterClient) CheckExistingSnapshot(snapName, volName string) (*api.SnapInfo, error) {
	snapInfo, err := gc.client.SnapshotInfo(snapName)
	if err != nil {
		errResp := gc.client.LastErrorResponse()
		//errResp will be nil in case of No route to host error
		if errResp != nil && errResp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("snapshot not found %v", err)
		}
		return nil, err
	}
	if snapInfo.ParentVolName != volName {
		return nil, fmt.Errorf("snapshot %s belongs to a different volume %s", snapName, snapInfo.ParentVolName)
	}

	if snapInfo.VolInfo.State != api.VolStarted {
		actReq := api.SnapActivateReq{
			Force: true,
		}
		err = gc.client.SnapshotActivate(actReq, snapName)
		if err != nil {
			return nil, fmt.Errorf("failed to activate snapshot: %v", err)
		}
	}

	ret := api.SnapInfo(snapInfo)
	return &ret, nil
}

// VolumeCreate creates a volume
func (gc *GlusterClient) VolumeCreate(volumeName string, volSizeMB uint64) error {
	glog.V(4).Infof("received request to create volume %s with size %d", volumeName, volSizeMB)
	volMetaMap := make(map[string]string)
	volMetaMap[volumeOwnerAnn] = glusterfsCSIDriverName
	volumeReq := api.VolCreateReq{
		Name:         volumeName,
		Metadata:     volMetaMap,
		ReplicaCount: defaultReplicaCount,
		Size:         volSizeMB,
	}

	glog.V(2).Infof("volume create request: %+v", volumeReq)
	volumeCreateResp, err := gc.client.VolumeCreate(volumeReq)
	if err != nil {
		return fmt.Errorf("failed to create volume: %v", err)
	}

	glog.V(3).Infof("volume create response: %+v", volumeCreateResp)
	err = gc.client.VolumeStart(volumeName, true)
	if err != nil {
		//we dont need to delete the volume if volume start fails
		//as we are listing the volumes and starting it again
		//before sending back the response
		return fmt.Errorf("failed to start volume: %v", err)
	}

	return nil
}

// SnapshotCreate create snapshot of an existing volume
func (gc *GlusterClient) SnapshotCreate(snapName, srcVol string) (*api.SnapInfo, error) {
	snapReq := api.SnapCreateReq{
		VolName:  srcVol,
		SnapName: snapName,
		Force:    true,
	}
	glog.V(2).Infof("snapshot request: %+v", snapReq)
	snapInfo, err := gc.client.SnapshotCreate(snapReq)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot %s: %v", snapName, err)
	}

	err = gc.client.SnapshotActivate(api.SnapActivateReq{Force: true}, snapName)
	if err != nil {
		return nil, fmt.Errorf("failed to activate snapshot %s: %v", snapName, err)
	}

	ret := api.SnapInfo(snapInfo)
	return &ret, nil
}

// SnapshotClone creates a clone of a snapshot
func (gc *GlusterClient) SnapshotClone(snapName, volName string) error {
	var snapreq api.SnapCloneReq

	glog.V(2).Infof("creating volume from snapshot %s", snapName)
	snapreq.CloneName = volName
	snapResp, err := gc.client.SnapshotClone(snapName, snapreq)
	if err != nil {
		return fmt.Errorf("failed to create volume clone: %v", err)
	}
	err = gc.client.VolumeStart(volName, true)
	if err != nil {
		return fmt.Errorf("failed to start volume: %v", err)
	}
	glog.V(1).Infof("snapshot clone response: %+v", snapResp)
	return nil
}

// Get retrieves a GlusterClient
func (gcc GlusterClients) Get(server, user string) (*GlusterClient, error) {
	glusterClientCacheMtx.Lock()
	defer glusterClientCacheMtx.Unlock()

	users, ok := gcc[server]
	if !ok {
		return nil, fmt.Errorf("Server %s not found in cache", server)
	}
	gc, ok := users[user]
	if !ok {
		return nil, fmt.Errorf("Client %s / %s not found in cache", server, user)
	}

	return gc, nil
}

// FindVolumeClient looks for a volume among current known servers
func (gcc GlusterClients) FindVolumeClient(volumeID string) (*GlusterClient, error) {
	var gc *GlusterClient

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	// Search all PVs for volumes from us
	pvs, err := clientset.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pvList := []corev1.PersistentVolume{}
	for i, pv := range pvs.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == glusterfsCSIDriverName {
			pvList = append(pvList, pv)

			if pv.Spec.CSI.VolumeHandle == volumeID {
				vol := pv.Spec.CSI
				url := vol.VolumeAttributes["glusterurl"]
				user := vol.VolumeAttributes["glusteruser"]
				gc, err = gcc.Get(url, user)
				if err != nil {
					glog.V(1).Infof(" Error getting GlusterClient %s / %s: %s", url, user, err)
					continue
				}
				break
			}
		}
		if i == len(pvs.Items)-1 {
			glog.Warningf("No PV found for volume %s", volumeID)
		}
	}

	// If volume not found, cycle through all discovered connections
	if gc == nil {
		// Update GlusterClient cache
		for _, pv := range pvList {
			attrs := pv.Spec.CSI.VolumeAttributes
			url := attrs["glusterurl"]
			user := attrs["glusteruser"]
			_, err = gcc.Get(url, user)
			if err != nil {
				glog.V(1).Infof("GlusterClient for %s / %s not found, initializing", url, user)

				searchClient := &GlusterClient{
					url:      url,
					username: user,
					password: attrs["glusterusersecret"],
				}
				err = gcc.Init(searchClient)
				if err != nil {
					glog.V(1).Infof("Error initializing GlusterClient %s / %s: %s", url, user, err)
					continue
				}
			}
		}

		// Check every connection for the volume
		for server, users := range gcc {
			for user, searchClient := range users {
				err = searchClient.CheckExistingVolume(volumeID, 0)
				if err != nil {
					glog.V(1).Infof("Error with GlusterClient %s / %s: %s", server, user, err)
					continue
				}

				gc = searchClient
				break
			}
			if gc != nil {
				break
			}
		}
	}

	if gc.client == nil {
		return nil, errVolumeNotFound
	}
	return gc, nil
}

// Init initializes a GlusterClient
func (gcc GlusterClients) Init(client *GlusterClient) error {
	glusterClientCacheMtx.Lock()
	defer glusterClientCacheMtx.Unlock()

	srvEnt, ok := gcc[client.url]
	if !ok {
		gcc[client.url] = make(map[string]*GlusterClient)
		srvEnt = gcc[client.url]
	}
	usrEnt, ok := srvEnt[client.username]
	if ok {
		client.password = SetStringIfEmpty(client.password, usrEnt.password)
		client.client = SetPointerIfEmpty(client.client, usrEnt.client).(*restclient.Client)
		client.cacert = SetStringIfEmpty(client.cacert, usrEnt.cacert)
		client.setInsecure(usrEnt.insecure)
	} else {
		glog.V(1).Infof("REST client %s / %s not found in cache, initializing", client.url, client.username)

		gd2, err := restclient.New(client.url, client.username, client.password, client.cacert, client.insecureBool)
		if err == nil {
			client.client = gd2
		} else {
			return fmt.Errorf("Failed to create REST client %s / %s: %s", client.url, client.username, err)
		}
	}

	srvEnt[client.username] = client

	return nil
}
