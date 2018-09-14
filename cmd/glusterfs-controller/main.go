package main

import (
	"github.com/gluster/gluster-csi-driver/pkg/command"
	"github.com/gluster/gluster-csi-driver/pkg/glusterfs"
)

// Driver Identifiers
const (
	cmdName          = "glusterfs-controller-driver"
	CSIDriverDesc    = "GlusterFS (glusterd2) CSI Controller Driver"
	CSIDriverName    = "org.gluster.glusterfs"
	CSIDriverVersion = "0.0.9"
)

func init() {
	command.Init()
}

func main() {
	config := &command.Config{
		Name:    CSIDriverName,
		Version: CSIDriverVersion,
		Desc:    CSIDriverDesc,
		CmdName: cmdName,
	}

	command.Main(config, glusterfs.New(config))
}
