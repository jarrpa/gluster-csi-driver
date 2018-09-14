package command

import (
	"flag"
	"os"

	"github.com/spf13/cobra"
)

// Config is the driver configuration struct
type Config struct {
	Endpoint string // CSI endpoint
	NodeID   string // CSI node ID
	Name     string // CSI driver name
	Version  string // CSI driver version
	Desc     string // CSI driver description
	CmdName  string // Executable name
}

// Driver interface
type Driver interface {
	Run()
}

// Init driver executable function
func Init() {
	// nolint: errcheck, gosec
	flag.Set("logtostderr", "true")
}

// Main driver executable function
func Main(config *Config, newDriver Driver) {
	// nolint: errcheck, gosec
	flag.CommandLine.Parse([]string{})

	cmd := &cobra.Command{
		Use:   config.Name,
		Short: config.Desc,
		Run: func(cmd *cobra.Command, args []string) {
			if config.NodeID == "" {
				config.NodeID = os.Getenv("CSI_NODE_ID")
			}
			if config.Endpoint == "" {
				config.Endpoint = os.Getenv("CSI_ENDPOINT")
			}
			newDriver.Run()
		},
	}

	cmd.Flags().AddGoFlagSet(flag.CommandLine)
	cmd.PersistentFlags().StringVar(&config.NodeID, "nodeid", "", "CSI node id")
	cmd.PersistentFlags().StringVar(&config.Endpoint, "endpoint", "", "CSI endpoint")

	if err := cmd.Execute(); err != nil {
		// nolint: errcheck, gosec
		os.Stderr.WriteString(err.Error())
		os.Exit(1)
	}
}
