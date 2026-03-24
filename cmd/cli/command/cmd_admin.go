package command

import (
	"fmt"

	"github.com/spf13/cobra"
)

var adminCmd = &cobra.Command{
	Use:   "admin",
	Short: "Administer users and system state (admin role required)",
	Long: `Admin commands require the 'admin' realm role in Keycloak.

Sub-commands:
  jobs    Manage all jobs across all users
  users   Manage Keycloak users`,
}

func init() {
	adminCmd.AddCommand(adminJobsCmd)
	adminCmd.AddCommand(adminUsersCmd)
}

var adminJobsCmd = &cobra.Command{
	Use:   "jobs",
	Short: "Manage all jobs across all users",
}

func init() {
	adminJobsCmd.AddCommand(adminJobsListCmd)
}

var adminJobsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all jobs in the system",
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		var jobs []map[string]interface{}
		if err := c.Get("/admin/jobs", &jobs); err != nil {
			return err
		}
		if flagJSON {
			printJSON(jobs)
			return nil
		}
		tw := newTabWriter()
		fmt.Fprintln(tw, "JOB ID\tOWNER\tSTATUS\tMAPPERS\tREDUCERS\tSUBMITTED")
		for _, j := range jobs {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%.0f\t%.0f\t%s\n",
				strField(j, "job_id"),
				strField(j, "owner_user_id"),
				colourStatus(strField(j, "status")),
				numField(j, "num_mappers"),
				numField(j, "num_reducers"),
				fmtTime(strField(j, "submitted_at")),
			)
		}
		return tw.Flush()
	},
}

var adminUsersCmd = &cobra.Command{
	Use:   "users",
	Short: "Manage Keycloak users",
}

func init() {
	adminUsersCmd.AddCommand(adminUsersListCmd)
	adminUsersCmd.AddCommand(adminUsersCreateCmd)
	adminUsersCmd.AddCommand(adminUsersDeleteCmd)
	adminUsersCmd.AddCommand(adminUsersRoleCmd)
}

// admin users list

var adminUsersListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all users in the realm",
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		var users []map[string]interface{}
		if err := c.Get("/admin/users", &users); err != nil {
			return err
		}
		if flagJSON {
			printJSON(users)
			return nil
		}
		tw := newTabWriter()
		fmt.Fprintln(tw, "KEYCLOAK ID\tUSERNAME\tEMAIL\tENABLED")
		for _, u := range users {
			enabled := "false"
			if b, ok := u["enabled"].(bool); ok && b {
				enabled = "true"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
				strField(u, "id"),
				strField(u, "username"),
				strField(u, "email"),
				enabled,
			)
		}
		return tw.Flush()
	},
}

// admin users create

var createUserFlags struct {
	username string
	email    string
	password string
	role     string
}

var adminUsersCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new user in Keycloak",
	Long: `Create a new user and assign them a realm role.

Examples:
  mapreduce admin users create --username bob --email bob@example.com --password secret
  mapreduce admin users create --username admin2 --email a@b.com --password x --role admin`,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		payload := map[string]string{
			"username": createUserFlags.username,
			"email":    createUserFlags.email,
			"password": createUserFlags.password,
			"role":     createUserFlags.role,
		}
		var resp map[string]interface{}
		if err := c.Post("/admin/users", payload, &resp); err != nil {
			return err
		}
		if flagJSON {
			printJSON(resp)
			return nil
		}
		fmt.Printf("User created.\n")
		fmt.Printf("  ID       : %s\n", strField(resp, "id"))
		fmt.Printf("  Username : %s\n", strField(resp, "username"))
		fmt.Printf("  Email    : %s\n", strField(resp, "email"))
		fmt.Printf("  Role     : %s\n", strField(resp, "role"))
		return nil
	},
}

func init() {
	adminUsersCreateCmd.Flags().StringVar(&createUserFlags.username, "username", "", "Username (required)")
	adminUsersCreateCmd.Flags().StringVar(&createUserFlags.email, "email", "", "Email address (required)")
	adminUsersCreateCmd.Flags().StringVar(&createUserFlags.password, "password", "", "Initial password (required)")
	adminUsersCreateCmd.Flags().StringVar(&createUserFlags.role, "role", "user", "Realm role: user or admin")
	_ = adminUsersCreateCmd.MarkFlagRequired("username")
	_ = adminUsersCreateCmd.MarkFlagRequired("email")
	_ = adminUsersCreateCmd.MarkFlagRequired("password")
}

// admin users delete

var adminUsersDeleteCmd = &cobra.Command{
	Use:   "delete <keycloak-user-id>",
	Short: "Delete a user from Keycloak",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		if err := c.Delete("/admin/users/"+args[0], nil); err != nil {
			return err
		}
		fmt.Printf("User %s deleted.\n", args[0])
		return nil
	},
}

// admin users role

var roleFlag string

var adminUsersRoleCmd = &cobra.Command{
	Use:   "role <keycloak-user-id>",
	Short: "Assign a realm role to a user",
	Long: `Assign a Keycloak realm role to an existing user.

Examples:
  mapreduce admin users role <id> --role admin
  mapreduce admin users role <id> --role user`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		var resp map[string]interface{}
		if err := c.Post("/admin/users/"+args[0]+"/roles",
			map[string]string{"role": roleFlag}, &resp); err != nil {
			return err
		}
		if flagJSON {
			printJSON(resp)
			return nil
		}
		fmt.Printf("Role '%s' assigned to user %s.\n", roleFlag, args[0])
		return nil
	},
}

func init() {
	adminUsersRoleCmd.Flags().StringVar(&roleFlag, "role", "", "Role to assign: user or admin (required)")
	_ = adminUsersRoleCmd.MarkFlagRequired("role")
}
