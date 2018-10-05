package heketi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/gluster/gluster-csi-driver/pkg/command"

	"github.com/golang/glog"
	hapi "github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/kubernetes-csi/csi-test/pkg/sanity"
	"k8s.io/kubernetes/pkg/util/mount"
)

const (
	CSIDriverName = "glusterfs.gluster.org"
)

var volumeCache = make(map[string]hapi.VolumeInfoResponse)
var TestVolName = "heketidbstorage"
var TestVolID = "heketidbstorage-heketi"
var TestVolSize = 1000

func TestDriverSuite(t *testing.T) {
	socket := "/tmp/csi.sock"
	endpoint := "unix://" + socket

	//cleanup socket file if already present
	os.Remove(socket)

	_, err := os.Create(socket)
	if err != nil {
		t.Fatal("Failed to create a socket file")
	}
	defer os.Remove(socket)

	volumeCache[TestVolID] = hapi.VolumeInfoResponse{
		VolumeInfo: hapi.VolumeInfo{
			VolumeCreateRequest: hapi.VolumeCreateRequest{
				Name: TestVolName,
				Size: TestVolSize,
			},
			Id: TestVolID,
		},
		Bricks: []hapi.BrickInfo{},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			handleGETRequest(w, r, t)

		case "DELETE":
			handleDELETERequest(w, r, t)

		case "POST":
			handlePOSTRequest(w, r, t)
		}
	}))
	defer ts.Close()

	url, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	config := &command.Config{
		Name:     CSIDriverName,
		Endpoint: endpoint,
		NodeID:   "testing",
		RestURL:  url.String(),
	}

	d := New(config, &mount.FakeMounter{})

	go d.Run()

	mntStageDir := "/tmp/mntStageDir"
	mntDir := "/tmp/mntDir"
	defer os.RemoveAll(mntStageDir)
	defer os.RemoveAll(mntDir)

	cfg := &sanity.Config{
		StagingPath: mntStageDir,
		TargetPath:  mntDir,
		Address:     endpoint,
	}

	sanity.Test(t, cfg)
}

func handleGETRequest(w http.ResponseWriter, r *http.Request, t *testing.T) {
	glog.Errorf("GET URL: %s", r.URL.String())
	if strings.HasSuffix(r.URL.String(), "/hello") {
		writeResp(w, http.StatusOK, nil, t)
		return
	}

	if strings.HasSuffix(r.URL.String(), "/volumes") {
		vols := []string{}
		for vol := range volumeCache {
			vols = append(vols, vol)
		}
		resp := hapi.VolumeListResponse{
			Volumes: vols,
		}
		writeResp(w, http.StatusOK, resp, t)
		return
	}

	if strings.HasSuffix(r.URL.String(), "/clusters") {
		resp := hapi.ClusterListResponse{
			Clusters: []string{"1"},
		}
		writeResp(w, http.StatusOK, resp, t)
		return
	}

	vol := strings.Split(strings.Trim(r.URL.String(), "/"), "/")
	if vol[0] == "volumes" {
		volID := vol[1]
		if resp, ok := volumeCache[volID]; ok {
			writeResp(w, http.StatusOK, resp, t)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}
	if vol[0] == "clusters" {
		resp := hapi.ClusterInfoResponse{
			Id: vol[1],
			Nodes: []string{
				"node1",
				"node2",
				"node3",
			},
			Volumes: []string{},
		}
		writeResp(w, http.StatusOK, resp, t)
		return
	}
	if vol[0] == "nodes" {
		resp := hapi.NodeInfoResponse{
			NodeInfo: hapi.NodeInfo{
				Id: vol[1],
				NodeAddRequest: hapi.NodeAddRequest{
					Hostnames: hapi.HostAddresses{
						Manage:  []string{vol[1]},
						Storage: []string{vol[1]},
					},
				},
			},
		}
		writeResp(w, http.StatusOK, resp, t)
		return
	}

	writeResp(w, http.StatusNotFound, fmt.Sprintf("GET 404: %v", r), t)
}

func handlePOSTRequest(w http.ResponseWriter, r *http.Request, t *testing.T) {
	glog.Errorf("POST URL: %v", r)
	//if strings.Contains(r.URL.String(), "/volumes") {
	if strings.HasSuffix(r.URL.String(), "/volumes") {
		defer r.Body.Close()

		var req hapi.VolumeCreateRequest
		json.NewDecoder(r.Body).Decode(&req)
		newID := fmt.Sprintf("%s-heketi", req.Name)
		resp := hapi.VolumeInfoResponse{
			VolumeInfo: hapi.VolumeInfo{
				VolumeCreateRequest: req,
				Id:                  newID,
			},
			Bricks: []hapi.BrickInfo{},
		}
		volumeCache[newID] = resp
		w.Header().Set("Location", fmt.Sprintf("/volumes/%s", newID))
		writeResp(w, http.StatusAccepted, nil, t)
		return
	}

	writeResp(w, http.StatusNotFound, fmt.Sprintf("POST 404: %v", r), t)
}

func handleDELETERequest(w http.ResponseWriter, r *http.Request, t *testing.T) {
	glog.Errorf("DELETE URL: %s", r.URL.String())
	vol := strings.Split(strings.Trim(r.URL.String(), "/"), "/")
	if vol[0] == "volumes" {
		volID := vol[1]
		delete(volumeCache, volID)
		w.Header().Set("Location", fmt.Sprintf("/volumes/%s", volID))
		writeResp(w, http.StatusAccepted, nil, t)
		return
	}
	writeResp(w, http.StatusNotFound, fmt.Sprintf("DELETE 404: %s", r.URL.String()), t)
}

func writeResp(w http.ResponseWriter, status int, resp interface{}, t *testing.T) {
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(&resp)
	if err != nil {
		t.Fatal("Failed to write response ", err)
	}
}
