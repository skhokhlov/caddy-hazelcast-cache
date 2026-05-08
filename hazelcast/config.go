package hazelcast

import (
	"errors"
	"time"
)

// Config is the user-facing configuration for the Hazelcast cache provider.
//
// The struct is JSON-friendly so it can be embedded inside Souin's generic
// `core.CacheProvider.Configuration` field; it carries no client state and may
// be safely deep-copied. Use Validate to apply defaults and reject obviously
// broken combinations before handing it to the client factory.
type Config struct {
	Addresses      []string      `json:"addresses,omitempty"`
	ClusterName    string        `json:"cluster_name,omitempty"`
	MapName        string        `json:"map_name,omitempty"`
	Username       string        `json:"username,omitempty"`
	Password       string        `json:"password,omitempty"`
	TLS            *TLSConfig    `json:"tls,omitempty"`
	ReadTimeout    time.Duration `json:"read_timeout,omitempty"`
	WriteTimeout   time.Duration `json:"write_timeout,omitempty"`
	ConnectTimeout time.Duration `json:"connect_timeout,omitempty"`
	SmartRouting   bool          `json:"smart_routing,omitempty"`
}

// TLSConfig describes how the client should authenticate the Hazelcast cluster
// and (optionally) itself. An empty TLSConfig means TLS is disabled; setting
// any trust or identity material enables it.
type TLSConfig struct {
	CACertFile         string `json:"ca_cert_file,omitempty"`
	CertFile           string `json:"cert_file,omitempty"`
	KeyFile            string `json:"key_file,omitempty"`
	ServerName         string `json:"server_name,omitempty"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
}

const (
	defaultClusterName = "dev"
	defaultMapName     = "souin-cache"
)

// ErrNoAddresses is returned by Validate when no member addresses were configured.
var ErrNoAddresses = errors.New("hazelcast: at least one address must be configured")

// Validate fills in defaults for unset optional fields and returns a typed
// error if the resulting configuration is not internally consistent.
func (c *Config) Validate() error {
	if len(c.Addresses) == 0 {
		return ErrNoAddresses
	}
	if c.ClusterName == "" {
		c.ClusterName = defaultClusterName
	}
	if c.MapName == "" {
		c.MapName = defaultMapName
	}
	if c.TLS != nil {
		if err := c.TLS.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (t *TLSConfig) validate() error {
	if t.CertFile != "" && t.KeyFile == "" {
		return errors.New("hazelcast: tls.cert_file requires tls.key_file")
	}
	if t.KeyFile != "" && t.CertFile == "" {
		return errors.New("hazelcast: tls.key_file requires tls.cert_file")
	}
	return nil
}
