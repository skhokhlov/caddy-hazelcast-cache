package hazelcast

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// insecureSkipVerifyEnv is the environment variable that gates
// TLSConfig.InsecureSkipVerify. The flag is intentionally hard to enable
// because it disables certificate verification entirely; setting it in
// configuration alone is treated as a misconfiguration.
const insecureSkipVerifyEnv = "CADDY_HAZELCAST_ALLOW_INSECURE_TLS"

// buildTLSConfig translates the user-facing TLSConfig into a *tls.Config
// suitable for the Hazelcast client. A nil or zero-valued TLSConfig disables
// TLS and returns (nil, nil); any non-zero field activates it.
func buildTLSConfig(t *TLSConfig) (*tls.Config, error) {
	if t == nil || (*t == TLSConfig{}) {
		return nil, nil
	}
	out := &tls.Config{
		ServerName: t.ServerName,
		MinVersion: tls.VersionTLS12,
	}
	if t.CACertFile != "" {
		pem, err := os.ReadFile(t.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("hazelcast: reading tls.ca_cert_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("hazelcast: tls.ca_cert_file %q contains no PEM certificates", t.CACertFile)
		}
		out.RootCAs = pool
	}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("hazelcast: loading tls.cert_file/tls.key_file: %w", err)
		}
		out.Certificates = []tls.Certificate{cert}
	}
	if t.InsecureSkipVerify {
		if os.Getenv(insecureSkipVerifyEnv) != "1" {
			return nil, fmt.Errorf("hazelcast: tls.insecure_skip_verify=true requires %s=1 in the environment", insecureSkipVerifyEnv)
		}
		out.InsecureSkipVerify = true
	}
	return out, nil
}
