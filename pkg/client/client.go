package client

import (
	"fmt"
	"sync"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var errNotFoundStr = "not found"
var errExistsStr = "already exists with a different"

type clientErr struct {
	errStr string
	vars   []string
}

func (f clientErr) Error() string {
	errFmt := ""
	if f.errStr == errNotFoundStr {
		errFmt = fmt.Sprintf("%s %s %s", f.vars[0], f.vars[1], f.errStr)
	} else if f.errStr == errExistsStr {
		errFmt = fmt.Sprintf("%s %s %s %s, %s", f.vars[0], f.vars[1], f.errStr, f.vars[2], f.vars[3])
	}

	return errFmt
}

func newClientErr(errStr string, vars ...string) clientErr {
	return clientErr{errStr, vars}
}

func matchClientErr(err error, errStr string) bool {
	cliErr, ok := err.(clientErr)
	return ok && cliErr.errStr == errStr
}

// ErrNotFound creates a new "not found" error
func ErrNotFound(kind, name string) error {
	return newClientErr(errNotFoundStr, kind, name)
}

// ErrExists creates a new "different owner" error
func ErrExists(kind, name, property, propVal string) error {
	return newClientErr(errExistsStr, kind, name, property, propVal)
}

// IsErrNotFound checks for an ErrNotFound
func IsErrNotFound(err error) bool {
	return matchClientErr(err, errNotFoundStr)
}

// IsErrExists checks for an ErrExists
func IsErrExists(err error) bool {
	return matchClientErr(err, errExistsStr)
}

// GlusterClient is an interface to clients for different Gluster server types
type GlusterClient interface {
	Update(map[string]string) (GlusterClient, error)
	GetClusterNodes(string) ([]string, error)
	CheckExistingVolume(string, int64) error
	CreateVolume(string, int64, map[string]string) error
	DeleteVolume(string) error
	ListVolumes() ([]*csi.Volume, error)
	CheckExistingSnapshot(string, string) (*csi.Snapshot, error)
	CreateSnapshot(string, string) (*csi.Snapshot, error)
	CloneSnapshot(string, string) error
	DeleteSnapshot(string) error
	ListSnapshots(string, string) ([]*csi.Snapshot, error)
}

// GlusterClients is a map of maps of GlusterClient structs. The first map is
// indexed by the server/url the GlusterClient connects to. The second map is
// indexed by the username used on that server.
type GlusterClients struct {
	Cache         map[string]map[string]GlusterClient
	DefaultClient GlusterClient
	Mtx           *sync.Mutex
}

// GlusterClientCache is a global GlusterClients cache
var GlusterClientCache = &GlusterClients{
	Cache: make(map[string]map[string]GlusterClient),
	Mtx:   &sync.Mutex{},
}

// FindVolumeClient looks for a volume among current known servers
func (gcc *GlusterClients) FindVolumeClient(volumeID, driverName string) (GlusterClient, error) {
	var gc GlusterClient

	// Search all PVs for volumes from us
	gc, err := gcc.findVolumeClientPV(volumeID, driverName)
	if err != nil && IsErrNotFound(err) {
		// If volume not found, check every connection for the volume
		glog.V(1).Infof("no PV found for volume %s", volumeID)
		gc = nil
		for server, users := range gcc.Cache {
			for user, searchClient := range users {
				checkErr := searchClient.CheckExistingVolume(volumeID, 0)
				if checkErr != nil && !IsErrNotFound(checkErr) {
					glog.V(1).Infof("error searching GlusterClient %s / %s for volume %s: %v", server, user, volumeID, err)
				} else if checkErr == nil {
					gc = searchClient
					err = nil
					break
				}
			}
			if gc != nil {
				break
			}
		}
	}

	return gc, err
}

func (gcc *GlusterClients) findVolumeClientPV(volumeID, driverName string) (GlusterClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	pvs, err := clientset.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, pv := range pvs.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == driverName {
			attrs := pv.Spec.CSI.VolumeAttributes
			url := attrs["glusterurl"]
			user := attrs["glusteruser"]

			// Attempt to get GlusterClient and update
			// GlusterClients cache
			searchClient, getErr := gcc.Get(attrs)
			if getErr != nil {
				getErr = fmt.Errorf("error getting GlusterClient %s / %s for volume %s: %v", url, user, volumeID, getErr)
				glog.V(1).Info(getErr.Error())
			}

			// At this point, the volume was successfully created
			// at bound to a PV in the past. If this fails, we
			// can't safely assume the volume has already been
			// deleted.
			if pv.Spec.CSI.VolumeHandle == volumeID {
				return searchClient, getErr
			}
		}
	}

	return nil, ErrNotFound("volume", volumeID)
}

// Get retrieves a GlusterClient from the cache. If one is not found, a new
// GlusterClient is created and cached.
func (gcc *GlusterClients) Get(params map[string]string) (GlusterClient, error) {
	var err error
	var client GlusterClient

	gcc.Mtx.Lock()
	defer gcc.Mtx.Unlock()

	url := params["glusterurl"]
	user := params["glusteruser"]

	srvEnt, ok := gcc.Cache[url]
	if !ok {
		gcc.Cache[url] = make(map[string]GlusterClient)
		srvEnt = gcc.Cache[url]
	}
	usrEnt, ok := srvEnt[user]
	if ok {
		glog.V(1).Infof("REST client %s / %s found in cache", url, user)
		client = usrEnt
	} else {
		glog.V(1).Infof("REST client %s / %s not found in cache", url, user)
		client = gcc.DefaultClient
	}

	client, err = client.Update(params)
	if err != nil {
		return nil, err
	}

	srvEnt[user] = client

	return client, nil
}
