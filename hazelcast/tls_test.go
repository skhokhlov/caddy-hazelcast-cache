package hazelcast

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTestCertAndKey(t *testing.T, dir, name string) (certPath, keyPath string, certPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath, certPEM
}

func TestBuildTLSConfigNilOrEmpty(t *testing.T) {
	tests := []struct {
		name string
		in   *TLSConfig
	}{
		{name: "nil", in: nil},
		{name: "zero", in: &TLSConfig{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildTLSConfig(tt.in)
			if err != nil {
				t.Fatalf("buildTLSConfig: %v", err)
			}
			if got != nil {
				t.Errorf("buildTLSConfig: got %+v, want nil", got)
			}
		})
	}
}

func TestBuildTLSConfigServerNameOnly(t *testing.T) {
	got, err := buildTLSConfig(&TLSConfig{ServerName: "hazelcast.internal"})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if got == nil {
		t.Fatalf("buildTLSConfig: got nil, want *tls.Config with ServerName set")
	}
	if got.ServerName != "hazelcast.internal" {
		t.Errorf("ServerName: got %q, want %q", got.ServerName, "hazelcast.internal")
	}
	if got.MinVersion < 0x0303 {
		t.Errorf("MinVersion: got %#x, want >= TLS1.2 (0x0303)", got.MinVersion)
	}
}

func TestBuildTLSConfigCAOnly(t *testing.T) {
	dir := t.TempDir()
	certPath, _, _ := writeTestCertAndKey(t, dir, "ca")

	got, err := buildTLSConfig(&TLSConfig{CACertFile: certPath, ServerName: "srv"})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if got.RootCAs == nil {
		t.Errorf("RootCAs: got nil, want non-nil")
	}
	if len(got.Certificates) != 0 {
		t.Errorf("Certificates: got %d, want 0", len(got.Certificates))
	}
}

func TestBuildTLSConfigMTLS(t *testing.T) {
	dir := t.TempDir()
	caPath, _, _ := writeTestCertAndKey(t, dir, "ca")
	clientCert, clientKey, _ := writeTestCertAndKey(t, dir, "client")

	got, err := buildTLSConfig(&TLSConfig{
		CACertFile: caPath,
		CertFile:   clientCert,
		KeyFile:    clientKey,
		ServerName: "srv",
	})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if got.RootCAs == nil {
		t.Errorf("RootCAs: got nil, want non-nil")
	}
	if len(got.Certificates) != 1 {
		t.Fatalf("Certificates: got %d, want 1", len(got.Certificates))
	}
	if got.ServerName != "srv" {
		t.Errorf("ServerName: got %q, want %q", got.ServerName, "srv")
	}
}

func TestBuildTLSConfigUnreadableCA(t *testing.T) {
	_, err := buildTLSConfig(&TLSConfig{CACertFile: "/nonexistent/path/ca.pem"})
	if err == nil {
		t.Fatal("buildTLSConfig: got nil error, want error for unreadable CA")
	}
	if !strings.Contains(err.Error(), "hazelcast:") {
		t.Errorf("error not prefixed with hazelcast: %v", err)
	}
}

func TestBuildTLSConfigBadPEM(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(bad, []byte("not a pem file"), 0o600); err != nil {
		t.Fatalf("write bad pem: %v", err)
	}
	_, err := buildTLSConfig(&TLSConfig{CACertFile: bad})
	if err == nil {
		t.Fatal("buildTLSConfig: got nil error, want error for malformed PEM")
	}
}

func TestBuildTLSConfigUnreadableClientCert(t *testing.T) {
	dir := t.TempDir()
	caPath, _, _ := writeTestCertAndKey(t, dir, "ca")
	_, err := buildTLSConfig(&TLSConfig{
		CACertFile: caPath,
		CertFile:   "/nonexistent/client.pem",
		KeyFile:    "/nonexistent/client.key",
	})
	if err == nil {
		t.Fatal("buildTLSConfig: got nil error, want error for unreadable client cert")
	}
}

func TestBuildTLSConfigInsecureSkipVerifyRequiresEnv(t *testing.T) {
	t.Setenv(insecureSkipVerifyEnv, "")
	_, err := buildTLSConfig(&TLSConfig{
		ServerName:         "srv",
		InsecureSkipVerify: true,
	})
	if err == nil {
		t.Fatal("buildTLSConfig: got nil error, want error without env opt-in")
	}
	if !strings.Contains(err.Error(), insecureSkipVerifyEnv) {
		t.Errorf("error should mention env var %q: %v", insecureSkipVerifyEnv, err)
	}
}

func TestBuildTLSConfigInsecureSkipVerifyWithEnv(t *testing.T) {
	t.Setenv(insecureSkipVerifyEnv, "1")
	got, err := buildTLSConfig(&TLSConfig{
		ServerName:         "srv",
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if !got.InsecureSkipVerify {
		t.Error("InsecureSkipVerify: got false, want true")
	}
}

func TestBuildClientConfigWiresSSL(t *testing.T) {
	dir := t.TempDir()
	caPath, _, _ := writeTestCertAndKey(t, dir, "ca")

	cfg := &Config{
		Addresses: []string{"hz-0:5701"},
		TLS: &TLSConfig{
			CACertFile: caPath,
			ServerName: "hazelcast.internal",
		},
	}
	hzCfg, err := buildClientConfig(cfg)
	if err != nil {
		t.Fatalf("buildClientConfig: %v", err)
	}
	if !hzCfg.Cluster.Network.SSL.Enabled {
		t.Error("hzCfg.Cluster.Network.SSL.Enabled: got false, want true")
	}
	if got := hzCfg.Cluster.Network.SSL.ServerName; got != "hazelcast.internal" {
		t.Errorf("SSL.ServerName: got %q, want %q", got, "hazelcast.internal")
	}
	tlsCfg := hzCfg.Cluster.Network.SSL.TLSConfig()
	if tlsCfg == nil || tlsCfg.RootCAs == nil {
		t.Error("expected RootCAs to be propagated to SSL.TLSConfig()")
	}
}

func TestBuildClientConfigNoTLSDisabled(t *testing.T) {
	cfg := &Config{Addresses: []string{"hz-0:5701"}}
	hzCfg, err := buildClientConfig(cfg)
	if err != nil {
		t.Fatalf("buildClientConfig: %v", err)
	}
	if hzCfg.Cluster.Network.SSL.Enabled {
		t.Error("hzCfg.Cluster.Network.SSL.Enabled: got true, want false when TLS unset")
	}
}
