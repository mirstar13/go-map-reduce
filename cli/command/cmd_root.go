package command

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	cliclient "github.com/mirstar13/go-map-reduce/cli/client"
	"github.com/mirstar13/go-map-reduce/cli/config"
)

// Global flags set on the root command and inherited by all subcommands.
var (
	flagServer string // --server  overrides config file value
	flagJSON   bool   // --output json
)

// cfg is loaded once at PersistentPreRun time.
var cfg *config.Config

// RootCmd is the top-level `mapreduce` command.
var RootCmd = &cobra.Command{
	Use:   "mapreduce",
	Short: "MapReduce platform CLI",
	Long: `mapreduce is the command-line interface for the MapReduce platform.

Commands:
  login          Authenticate and save a token
  jobs           Submit and manage MapReduce jobs
  admin          Administer users and system state (admin role required)

Examples:
  mapreduce login --server http://localhost:8081 --username alice
  mapreduce jobs submit --input data.jsonl --mapper map.py --reducer reduce.py
  mapreduce jobs list
  mapreduce admin users create --username bob --email bob@example.com`,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Skip config loading for the login command itself.
		if cmd.Name() == "login" {
			return nil
		}
		var err error
		cfg, err = config.Load()
		if err != nil {
			return err
		}
		// --server flag overrides config file.
		if flagServer != "" {
			cfg.ServerURL = flagServer
		}
		if cfg.ServerURL == "" {
			return fmt.Errorf("no server URL configured — run: mapreduce login --server <url>")
		}
		if cfg.Token == "" {
			return fmt.Errorf("not logged in — run: mapreduce login")
		}
		return nil
	},
}

func init() {
	RootCmd.PersistentFlags().StringVar(&flagServer, "server", "", "UI service base URL (overrides saved config)")
	RootCmd.PersistentFlags().BoolVar(&flagJSON, "json", false, "Output raw JSON instead of a table")

	RootCmd.AddCommand(loginCmd)
	RootCmd.AddCommand(jobsCmd)
	RootCmd.AddCommand(adminCmd)
}

// newClient builds an authenticated client from the loaded config.
func newClient() *cliclient.Client {
	return cliclient.New(cfg.ServerURL, cfg.Token)
}

// newUnauthClient builds a client without a token (for login).
func newUnauthClient(serverURL string) *cliclient.Client {
	return cliclient.New(serverURL, "")
}

// printJSON pretty-prints v as JSON to stdout.
func printJSON(v interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// newTabWriter returns a tab-aligned writer suitable for table output.
func newTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
}

// printError writes a formatted error to stderr and exits with code 1.
func printError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
}
