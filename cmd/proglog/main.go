package main

import (
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/koneal2013/proglog/internal/agent"
	"github.com/koneal2013/proglog/internal/config"
)

type cli struct {
	cfg cfg
}

func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {
	if configFile, err := cmd.Flags().GetString("config-file"); err != nil {
		return err
	} else {
		viper.SetConfigFile(configFile)
		if err = viper.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return err
			}
		}
		c.cfg.DataDir = viper.GetString("data-dir")
		c.cfg.NodeName = viper.GetString("node-name")
		c.cfg.BindAddr = viper.GetString("bind-addr")
		c.cfg.RPCPort = viper.GetInt("rpc-port")
		c.cfg.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
		c.cfg.Bootstrap = viper.GetBool("bootstrap")
		c.cfg.ACLModelFile = viper.GetString("acl-model-file")
		c.cfg.ACLPolicyFile = viper.GetString("acl-policy-file")
		c.cfg.ServerTLSConfig.CertFile = viper.GetString("server-tls-cert-file")
		c.cfg.ServerTLSConfig.KeyFile = viper.GetString("server-tls-key-file")
		c.cfg.ServerTLSConfig.CAFile = viper.GetString("server-tls-ca-file")
		c.cfg.PeerTLSConfig.CertFile = viper.GetString("peer-tls-cert-file")
		c.cfg.PeerTLSConfig.KeyFile = viper.GetString("peer-tls-key-file")
		c.cfg.PeerTLSConfig.CAFile = viper.GetString("peer-tls-ca-file")

		if c.cfg.ServerTLSConfig.CertFile != "" &&
			c.cfg.ServerTLSConfig.KeyFile != "" {
			c.cfg.ServerTLSConfig.Server = true
			c.cfg.Config.ServerTLSConfig, err = config.SetupTLSConfig(c.cfg.ServerTLSConfig)
			if err != nil {
				return err
			}
		}
		if c.cfg.PeerTLSConfig.CertFile != "" &&
			c.cfg.PeerTLSConfig.KeyFile != "" {
			c.cfg.Config.PeerTLSConfig, err = config.SetupTLSConfig(c.cfg.PeerTLSConfig)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	if agent, err := agent.New(c.cfg.Config); err != nil {
		return err
	} else {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		<-sigc
		return agent.Shutdown()
	}
}

type cfg struct {
	agent.Config
	ServerTLSConfig config.TLSConfig
	PeerTLSConfig   config.TLSConfig
}

func setupFlags(cmd *cobra.Command) error {
	if hostname, err := os.Hostname(); err != nil {
		log.Fatal(err)
	} else {
		cmd.Flags().String("config-file", "", "Path to config file.")
		dataDir := path.Join(os.TempDir(), "proglog")
		cmd.Flags().String("data-dir", dataDir, "Directory to store log and Raft data.")
		cmd.Flags().String("node-name", hostname, "Unique server ID.")
		cmd.Flags().String("bind-addr", "127.0.0.1:8401", "Address to bind serf on.")
		cmd.Flags().Int("rpc-port", 8400, "Port for PRC clients (and Raft) connections")
		cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
		cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")
		cmd.Flags().String("acl-model-file", "", "Path to ACL model.")
		cmd.Flags().String("act-policy-file", "", "Path to ACL policy.")
		cmd.Flags().String("server-tls-cert-file", "", "Path to server tls cert.")
		cmd.Flags().String("server-tls-key-file", "", "Path to server tls key.")
		cmd.Flags().String("server-tls-ca-file", "", "Path to server certificate authority.")
		cmd.Flags().String("peer-tls-cert-file", "", "Path to peer tls cert.")
		cmd.Flags().String("peer-tls-key-file", "", "Path to peer tls key.")
		cmd.Flags().String("peer-tls-ca-file", "", "Path to peer certificate authority.")
		cmd.Flags().String("optl-collector-url", "localhost:4317", "Url for OTPL tracing collector.")
		cmd.Flags().Bool("otpl-collector-insecure", true, "Flag to enable insecure mode for OTPL Collector.")
		cmd.Flags().Bool("is-development", false, "Flag to set log level.")
		return viper.BindPFlags(cmd.Flags())
	}
	return nil
}

func main() {
	cli := &cli{}

	cmd := &cobra.Command{
		Use:     "proglog",
		PreRunE: cli.setupConfig,
		RunE:    cli.run,
	}
	if err := setupFlags(cmd); err != nil {
		log.Fatal(err)
	}
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
