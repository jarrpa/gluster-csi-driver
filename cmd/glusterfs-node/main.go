package main

import (
	"github.com/gluster/gluster-csi-driver/pkg/command"
	"github.com/gluster/gluster-csi-driver/pkg/node"
)

// Driver Identifiers
const (
	cmdName          = "glusterfs-node-driver"
	CSIDriverDesc    = "GlusterFS (glusterd2) CSI Node Driver"
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

	command.Main(config, node.New(config))
}
