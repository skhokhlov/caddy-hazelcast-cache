package hazelcast

import (
	"context"
	"errors"
	"fmt"

	hzclient "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// ErrNilConfig is returned by the client factory when invoked without a Config.
var ErrNilConfig = errors.New("hazelcast: nil config")

// newClient validates cfg, translates it into a Hazelcast client configuration
// and starts a connected client. The returned client must be shut down by the
// caller. TLS wiring lands in step 1.3; this factory handles addresses,
// cluster name, credentials and connect timeout only.
func newClient(ctx context.Context, cfg *Config, log logger.Logger) (*hzclient.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	hzCfg, err := buildClientConfig(cfg)
	if err != nil {
		return nil, err
	}
	if log != nil {
		hzCfg.Logger.CustomLogger = log
	}
	client, err := hzclient.StartNewClientWithConfig(ctx, hzCfg)
	if err != nil {
		return nil, fmt.Errorf("hazelcast: starting client: %w", err)
	}
	return client, nil
}

// buildClientConfig validates cfg (applying defaults in place) and returns the
// equivalent upstream Hazelcast client configuration. Split from newClient so
// the translation can be unit-tested without spinning up a cluster.
func buildClientConfig(cfg *Config) (hzclient.Config, error) {
	if cfg == nil {
		return hzclient.Config{}, ErrNilConfig
	}
	if err := cfg.Validate(); err != nil {
		return hzclient.Config{}, err
	}
	hzCfg := hzclient.NewConfig()
	hzCfg.Cluster.Name = cfg.ClusterName
	addrs := make([]string, len(cfg.Addresses))
	copy(addrs, cfg.Addresses)
	hzCfg.Cluster.Network.Addresses = addrs
	if cfg.ConnectTimeout > 0 {
		hzCfg.Cluster.Network.ConnectionTimeout = types.Duration(cfg.ConnectTimeout)
	}
	if cfg.Username != "" || cfg.Password != "" {
		hzCfg.Cluster.Security.Credentials = cluster.CredentialsConfig{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}
	if cfg.SmartRouting != nil {
		hzCfg.Cluster.Unisocket = !*cfg.SmartRouting
	}
	return hzCfg, nil
}
