package glusterfs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gluster/gluster-csi-driver/pkg/glusterfs/utils"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/gluster/glusterd2/pkg/api"
	gd2Error "github.com/gluster/glusterd2/pkg/errors"
	"github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	volumeOwnerAnn            = "VolumeOwner"
	defaultVolumeSize   int64 = 1000 * utils.MB // default volume size ie 1 GB
	defaultReplicaCount       = 3
)

var errVolumeNotFound = errors.New("volume not found")

// ControllerServer struct of GlusterFS CSI driver with supported methods of CSI controller server spec.
type ControllerServer struct {
	*GfDriver
}

// CsiDrvParam stores csi driver specific request parameters.
// This struct will be used to gather specific fields of CSI driver:
// For eg. csiDrvName, csiDrvVersion..etc
// and also gather parameters passed from SC which not part of gluster volcreate api.
// GlusterCluster - The resturl of gluster cluster
// GlusterUser - The gluster username who got access to the APIs.
// GlusterUserToken - The password/token of glusterUser to connect to glusterCluster
// GlusterVersion - Says the version of the glustercluster running in glusterCluster endpoint.
// All of these fields are optional and can be used if needed.
type CsiDrvParam struct {
	GlusterCluster   string
	GlusterUser      string
	GlusterUserToken string
	GlusterVersion   string
	CsiDrvName       string
	CsiDrvVersion    string
}

// ProvisionerConfig is the combined configuration of gluster cli vol create request and CSI driver specific input
type ProvisionerConfig struct {
	gdVolReq *api.VolCreateReq
	*CsiDrvParam
}

// ParseCreateVolRequest parse incoming volume create request and fill ProvisionerConfig.
func (cs *ControllerServer) ParseCreateVolRequest(req *csi.CreateVolumeRequest) (*ProvisionerConfig, error) {
	var reqConf ProvisionerConfig

	if req != nil {
		params := req.GetParameters()
		gdReq := api.VolCreateReq{}

		gdReq.Name = req.Name

		reqConf.gdVolReq = &gdReq
		reqConf.GlusterCluster = params["glusterurl"]
		reqConf.GlusterUser = params["glusteruser"]
		reqConf.GlusterUserToken = params["glusterusersecret"]
	}

	return &reqConf, nil
}

// CreateVolume creates and starts the volume
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	glog.V(2).Infof("request received %+v", req)

	if err := cs.validateCreateVolumeReq(req); err != nil {
		return nil, err
	}

	glog.V(1).Infof("creating volume with name %s", req.Name)

	volSizeBytes := cs.getVolumeSize(req)
	volSizeMB := uint64(utils.RoundUpSize(volSizeBytes, 1024*1024))

	// parse the request.
	reqConf, err := cs.ParseCreateVolRequest(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Failed to parse request: %v", err)
	}

	volumeName := reqConf.gdVolReq.Name

	gc := &GlusterClient{
		url:      reqConf.GlusterCluster,
		username: reqConf.GlusterUser,
		password: reqConf.GlusterUserToken,
	}
	err = glusterClientCache.Init(gc)
	if err != nil {
		glog.Error(err)
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	err = gc.CheckExistingVolume(volumeName, volSizeMB)
	if err != nil {
		if err != errVolumeNotFound {
			glog.Error(err.Error())
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		snapName := req.VolumeContentSource.GetSnapshot().GetId()
		if snapName != "" {
			_, err = gc.CheckExistingSnapshot(snapName, volumeName)
			if err != nil {
				glog.Error(err.Error())
				code := codes.Internal
				if strings.Contains(err.Error(), "different volume") {
					code = codes.AlreadyExists
				}
				if strings.Contains(err.Error(), "not found") {
					code = codes.NotFound
				}
				return nil, status.Error(code, err.Error())
			}

			err = gc.SnapshotClone(snapName, volumeName)
			if err != nil {
				glog.Error(err.Error())
				return nil, status.Error(codes.Internal, err.Error())
			}
		} else {
			// If volume does not exist, provision volume
			err = gc.VolumeCreate(volumeName, volSizeMB)
			if err != nil {
				glog.Error(err.Error())
				return nil, status.Error(codes.Internal, err.Error())
			}
		}
	}

	glusterServer, bkpServers, err := gc.GetClusterNodes()
	if err != nil {
		glog.Errorf("failed to get cluster nodes for %s / %s: %v", gc.url, gc.username, err)
		return nil, status.Errorf(codes.Internal, "failed to get cluster nodes for %s / %s: %v", gc.url, gc.username, err)
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            volumeName,
			CapacityBytes: volSizeBytes,
			Attributes: map[string]string{
				"glustervol":        volumeName,
				"glusterserver":     glusterServer,
				"glusterbkpservers": strings.Join(bkpServers, ":"),
				"glusterurl":        reqConf.GlusterCluster,
				"glusteruser":       reqConf.GlusterUser,
				"glusterusersecret": reqConf.GlusterUserToken,
			},
		},
	}

	glog.V(4).Infof("CSI volume response: %+v", resp)
	return resp, nil
}

func (cs *ControllerServer) getVolumeSize(req *csi.CreateVolumeRequest) int64 {
	// If capacity mentioned, pick that or use default size 1 GB
	volSizeBytes := defaultVolumeSize
	if capRange := req.GetCapacityRange(); capRange != nil {
		volSizeBytes = capRange.GetRequiredBytes()
	}
	return volSizeBytes
}

func (cs *ControllerServer) validateCreateVolumeReq(req *csi.CreateVolumeRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request cannot be empty")
	}

	if req.GetName() == "" {
		return status.Error(codes.InvalidArgument, "name is a required field")
	}

	if reqCaps := req.GetVolumeCapabilities(); reqCaps == nil {
		return status.Error(codes.InvalidArgument, "volume capabilities is a required field")
	}

	return nil
}

// DeleteVolume deletes the given volume.
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "volume delete request is nil")
	}

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is nil")
	}
	glog.V(2).Infof("deleting volume with ID: %s", volumeID)

	gc, err := glusterClientCache.FindVolumeClient(volumeID)
	if err != nil && err != errVolumeNotFound {
		glog.Error(err)
		return nil, status.Errorf(codes.Internal, "error deleting volume %s: %s", volumeID, err.Error())
	} else if err == nil {
		err := gc.client.VolumeStop(volumeID)
		if err != nil && err.Error() != gd2Error.ErrVolAlreadyStopped.Error() {
			errResp := gc.client.LastErrorResponse()
			//errResp will be nil in case of 'No route to host' error
			if errResp != nil && errResp.StatusCode == http.StatusNotFound {
				return &csi.DeleteVolumeResponse{}, nil
			}
			glog.Errorf("failed to stop volume %s: %v", volumeID, err)
			return nil, status.Errorf(codes.Internal, "failed to stop volume %s: %v", volumeID, err)
		}

		err = gc.client.VolumeDelete(volumeID)
		if err != nil && err != errVolumeNotFound {
			errResp := gc.client.LastErrorResponse()
			//errResp will be nil in case of 'No route to host' error
			if errResp != nil && errResp.StatusCode == http.StatusNotFound {
				return &csi.DeleteVolumeResponse{}, nil
			}
			glog.Errorf("error deleting volume %s: %v", volumeID, err)
			return nil, status.Errorf(codes.Internal, "error deleting volume %s: %v", volumeID, err)
		}
		glog.Warningf("volume %s not found: %s", volumeID, err)
	}

	glog.Infof("successfully deleted volume %s", volumeID)
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume return Unimplemented error
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerUnpublishVolume return Unimplemented error
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ValidateVolumeCapabilities checks whether the volume capabilities requested
// are supported.
func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "ValidateVolumeCapabilities() - request is nil")
	}

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities() - VolumeId is nil")
	}

	reqCaps := req.GetVolumeCapabilities()
	if reqCaps == nil {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities() - VolumeCapabilities is nil")
	}

	_, err := glusterClientCache.FindVolumeClient(volumeID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "ValidateVolumeCapabilities() - %v", err)
	}

	var vcaps []*csi.VolumeCapability_AccessMode
	for _, mode := range []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	} {
		vcaps = append(vcaps, &csi.VolumeCapability_AccessMode{Mode: mode})
	}
	capSupport := true
	IsSupport := func(mode csi.VolumeCapability_AccessMode_Mode) bool {
		for _, m := range vcaps {
			if mode == m.Mode {
				return true
			}
		}
		return false
	}
	for _, cap := range reqCaps {
		if !IsSupport(cap.AccessMode.Mode) {
			capSupport = false
		}
	}

	resp := &csi.ValidateVolumeCapabilitiesResponse{
		Supported: capSupport,
	}
	glog.V(1).Infof("GlusterFS CSI driver volume capabilities: %+v", resp)
	return resp, nil
}

// ListVolumes returns a list of volumes
func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	entries := []*csi.ListVolumesResponse_Entry{}

	start, err := strconv.Atoi(req.StartingToken)
	if err != nil {
		glog.Errorf("Invalid StartingToken: %s", req.StartingToken)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid StartingToken: %s", req.StartingToken)
	}
	end := int32(start) + req.MaxEntries - 1

	for server, users := range glusterClientCache {
		for user, gc := range users {
			client := gc.client

			vols, err := client.Volumes("")
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}

			glusterServer, bkpServers, err := gc.GetClusterNodes()
			if err != nil {
				return nil, status.Errorf(codes.Internal, "Failed to get cluster nodes for %s / %s: %v", server, user, err)
			}

			for _, vol := range vols {
				if err != nil {
					glog.V(1).Infof("Error getting volume %s status from %s / %s: %s", vol.Name, server, user, err)
					continue
				}
				entries = append(entries, &csi.ListVolumesResponse_Entry{
					Volume: &csi.Volume{
						Id:            vol.Name,
						CapacityBytes: (int64(vol.Capacity)) * utils.MB,
						Attributes: map[string]string{
							"glustervol":        vol.Name,
							"glusterserver":     glusterServer,
							"glusterbkpservers": strings.Join(bkpServers, ":"),
							"glusterurl":        gc.url,
							"glusteruser":       gc.username,
							"glusterusersecret": gc.password,
						},
					},
				})
			}
		}
	}

	resp := &csi.ListVolumesResponse{
		Entries:   entries[start:end],
		NextToken: string(end),
	}
	return resp, nil
}

// GetCapacity returns the capacity of the storage pool
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerGetCapabilities returns the capabilities of the controller service.
func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	newCap := func(cap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
	}

	var caps []*csi.ControllerServiceCapability
	for _, cap := range []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	} {
		caps = append(caps, newCap(cap))
	}

	resp := &csi.ControllerGetCapabilitiesResponse{
		Capabilities: caps,
	}

	return resp, nil
}

// CreateSnapshot create snapshot of an existing PV
func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {

	if err := cs.validateCreateSnapshotReq(req); err != nil {
		return nil, err
	}

	snapName := req.GetName()
	srcVol := req.GetSourceVolumeId()

	glog.V(2).Infof("received request to create snapshot %s from volume %s", snapName, srcVol)

	gc, err := glusterClientCache.FindVolumeClient(srcVol)
	if err != nil {
		glog.Error(err)
		return nil, status.Errorf(codes.Internal, "error finding volume %s for snapshot %s: %v", srcVol, snapName, err)
	}

	snapInfo, err := gc.CheckExistingSnapshot(snapName, srcVol)
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			code := codes.Internal
			if strings.Contains(err.Error(), "different volume") {
				code = codes.AlreadyExists
			}
			glog.Error(err.Error())
			return nil, status.Error(code, err.Error())
		}

		snapInfo, err = gc.SnapshotCreate(snapName, srcVol)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			Id:             snapInfo.VolInfo.Name,
			SourceVolumeId: snapInfo.ParentVolName,
			CreatedAt:      snapInfo.CreatedAt.Unix(),
			SizeBytes:      (int64(snapInfo.VolInfo.Capacity)) * utils.MB,
			Status: &csi.SnapshotStatus{
				Type: csi.SnapshotStatus_READY,
			},
		},
	}, nil
}

func (cs *ControllerServer) validateCreateSnapshotReq(req *csi.CreateSnapshotRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "CreateSnapshot request is nil")
	}
	if req.GetName() == "" {
		return status.Error(codes.InvalidArgument, "CreateSnapshot - name cannot be nil")
	}

	if req.GetSourceVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "CreateSnapshot - sourceVolumeId is nil")
	}
	if req.GetName() == req.GetSourceVolumeId() {
		//In glusterd2 we cannot create a snapshot as same name as volume name
		return status.Error(codes.InvalidArgument, "CreateSnapshot - sourceVolumeId  and snapshot name cannot be same")
	}
	return nil
}

// DeleteSnapshot delete provided snapshot of a PV
func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "DeleteSnapshot request is nil")
	}
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "DeleteSnapshot - snapshotId is empty")
	}
	glog.V(4).Infof("deleting snapshot %s", req.GetSnapshotId())

	snapName := req.GetSnapshotId()

	gc, err := glusterClientCache.FindVolumeClient(snapName)
	if err != nil && err != errVolumeNotFound {
		glog.Error(err)
		return nil, status.Errorf(codes.Internal, "error deleting snapshot %s: %v", snapName, err)
	} else if err == nil {
		err := gc.client.SnapshotDeactivate(snapName)
		if err != nil {
			errResp := gc.client.LastErrorResponse()
			//if errResp != nil && errResp.StatusCode != http.StatusNotFound && err.Error() != gd2Error.ErrSnapDeactivated.Error() {
			if errResp != nil && errResp.StatusCode != http.StatusNotFound {
				glog.Errorf("failed to deactivate snapshot %s: %v", snapName, err)
				return nil, status.Errorf(codes.Internal, "DeleteSnapshot - failed to deactivate snapshot %s: %v", snapName, err)
			}
		}
		err = gc.client.SnapshotDelete(snapName)
		if err != nil {
			errResp := gc.client.LastErrorResponse()
			if errResp != nil && errResp.StatusCode != http.StatusNotFound {
				glog.Errorf("failed to delete snapshot %s: %v", snapName, err)
				return nil, status.Errorf(codes.Internal, "DeleteSnapshot - failed to delete snapshot %s: %v", snapName, err)
			}
		}
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots list the snapshots of a PV
func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	var startToken int32

	if req.GetStartingToken() != "" {
		i, parseErr := strconv.ParseUint(req.GetStartingToken(), 10, 32)
		if parseErr != nil {
			return nil, status.Errorf(codes.Aborted, "invalid starting token %s", parseErr.Error())
		}
		startToken = int32(i)
	}

	snaplist := api.SnapListResp{}
	snapName := req.GetSnapshotId()
	srcVol := req.GetSourceVolumeId()

	for _, users := range glusterClientCache {
		for _, gc := range users {
			client := gc.client

			if len(snapName) != 0 {
				snap, err := gc.CheckExistingSnapshot(snapName, srcVol)
				if err == nil {
					snaplist = append(snaplist, api.SnapList{ParentName: snap.ParentVolName, SnapList: []api.SnapInfo{*snap}})
				}
			} else if len(srcVol) != 0 {
				snaps, err := client.SnapshotList(srcVol)
				if err != nil {
					errResp := client.LastErrorResponse()
					if errResp != nil && errResp.StatusCode != http.StatusNotFound {
						glog.Errorf("failed to list snapshots: %v", err)
					}
				}
				snaplist = append(snaplist, snaps...)
			} else {
				//Get all snapshots
				snaps, err := client.SnapshotList("")
				if err != nil {
					glog.Errorf("failed to list snapshots: %v", err)
					return nil, status.Errorf(codes.Internal, "failed to get snapshots %s", err.Error())
				}
				snaplist = append(snaplist, snaps...)
			}
		}
	}

	return cs.doPagination(req, snaplist, startToken)
}

func (cs *ControllerServer) doPagination(req *csi.ListSnapshotsRequest, snapList api.SnapListResp, startToken int32) (*csi.ListSnapshotsResponse, error) {
	if req.GetStartingToken() != "" && int(startToken) > len(snapList) {
		return nil, status.Error(codes.Aborted, "invalid starting token")
	}

	var entries []*csi.ListSnapshotsResponse_Entry
	for _, snap := range snapList {
		for _, s := range snap.SnapList {
			entries = append(entries, &csi.ListSnapshotsResponse_Entry{
				Snapshot: &csi.Snapshot{
					Id:             s.VolInfo.Name,
					SourceVolumeId: snap.ParentName,
					CreatedAt:      s.CreatedAt.Unix(),
					SizeBytes:      (int64(s.VolInfo.Capacity)) * utils.MB,
					Status: &csi.SnapshotStatus{
						Type: csi.SnapshotStatus_READY,
					},
				},
			})
		}

	}

	//TODO need to remove paginating code once  glusterd2 issue
	//https://github.com/gluster/glusterd2/issues/372 is merged
	var (
		maximumEntries   = req.MaxEntries
		nextToken        int32
		remainingEntries = int32(len(snapList)) - startToken
		resp             csi.ListSnapshotsResponse
	)

	if maximumEntries == 0 || maximumEntries > remainingEntries {
		maximumEntries = remainingEntries
	}

	resp.Entries = entries[startToken : startToken+maximumEntries]

	if nextToken = startToken + maximumEntries; nextToken < int32(len(snapList)) {
		resp.NextToken = fmt.Sprintf("%d", nextToken)
	}
	return &resp, nil
}
