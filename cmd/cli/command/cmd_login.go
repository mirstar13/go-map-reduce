package command

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/mirstar13/go-map-reduce/cmd/cli/config"
)

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Authenticate and save a token",
	Long: `Authenticate against the MapReduce platform and save the access token
to ~/.mapreduce/config.json for use by subsequent commands.

Examples:
  mapreduce login --server http://localhost:8081 --username alice
  mapreduce login --server http://localhost:8081 --username admin --password secret`,
	RunE: runLogin,
}

var (
	loginServer   string
	loginUsername string
	loginPassword string
)

func init() {
	loginCmd.Flags().StringVar(&loginServer, "server", "", "UI service base URL (required)")
	loginCmd.Flags().StringVar(&loginUsername, "username", "", "Username (required)")
	loginCmd.Flags().StringVar(&loginPassword, "password", "", "Password (prompted if omitted)")
	_ = loginCmd.MarkFlagRequired("server")
	_ = loginCmd.MarkFlagRequired("username")
}

func runLogin(cmd *cobra.Command, args []string) error {
	password := loginPassword
	if password == "" {
		fmt.Printf("Password: ")
		raw, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println() // newline after hidden input
		if err != nil {
			return fmt.Errorf("read password: %w", err)
		}
		password = string(raw)
	}

	c := newUnauthClient(loginServer)

	var resp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := c.PostNoAuth("/auth/login", map[string]string{
		"username": loginUsername,
		"password": password,
	}, &resp); err != nil {
		return fmt.Errorf("login failed: %w", err)
	}

	// Persist to config file.
	cfg := &config.Config{
		ServerURL: loginServer,
		Token:     resp.AccessToken,
	}
	if err := cfg.Save(); err != nil {
		return fmt.Errorf("save config: %w", err)
	}

	fmt.Printf("Logged in as %s. Token expires in %ds.\n", loginUsername, resp.ExpiresIn)
	fmt.Printf("Config saved to ~/.mapreduce/config.json\n")
	return nil
}
