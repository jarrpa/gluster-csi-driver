package heketi

import (
	"github.com/gluster/gluster-csi-driver/pkg/command"
	"github.com/gluster/gluster-csi-driver/pkg/controller"

	"github.com/golang/glog"
)

// Driver implements command.Driver
type Driver struct {
	*command.Config
}

// New returns a new Driver
func New(config *command.Config) *Driver {
	hd := &Driver{}

	if config != nil {
		hd.Config = config
	} else {
		glog.Error("failed to initialize Heketi driver: config is nil")
		return nil
	}

	glog.V(1).Infof("%s initialized", hd.Desc)

	return hd
}

// Run runs the driver
func (d *Driver) Run() {
	SetDefaultClient(d.Name)
	controller.Run(d.Config)
}
