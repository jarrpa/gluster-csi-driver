package main

import (
	"github.com/gluster/gluster-csi-driver/pkg/command"
	"github.com/gluster/gluster-csi-driver/pkg/heketi"
)

// Driver Identifiers
const (
	cmdName          = "heketi-controller-driver"
	CSIDriverDesc    = "GlusterFS (heketi) CSI Controller Driver"
	CSIDriverName    = "org.gluster.glusterfs"
	CSIDriverVersion = "0.0.8"
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

	command.Main(config, heketi.New(config))
}
