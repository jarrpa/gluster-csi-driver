package main

import (
	"fmt"
	"os"

	"github.com/gluster/gluster-csi-driver/pkg/command"
	"github.com/gluster/gluster-csi-driver/pkg/heketi"
)

// Driver Identifiers
const (
	cmdName          = "heketi-csi-driver"
	CSIDriverDesc    = "GlusterFS (heketi) CSI Driver"
	CSIDriverName    = "org.gluster.heketi"
	CSIDriverVersion = "0.0.9"
)

func init() {
	command.Init()
}

func main() {
	var config = command.NewConfig(cmdName, CSIDriverName, CSIDriverVersion, CSIDriverDesc)

	d := heketi.New(config, nil)
	if d == nil {
		fmt.Println("Failed to initialize Heketi CSI driver")
		os.Exit(1)
	}

	command.Run(config, d)
}
