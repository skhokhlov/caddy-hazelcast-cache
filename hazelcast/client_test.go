package hazelcast

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestNewClientNilConfig(t *testing.T) {
	_, err := newClient(context.Background(), nil, nil)
	if !errors.Is(err, ErrNilConfig) {
		t.Fatalf("newClient with nil cfg: err = %v, want %v", err, ErrNilConfig)
	}
}

func TestNewClientPropagatesValidationError(t *testing.T) {
	_, err := newClient(context.Background(), &Config{}, nil)
	if !errors.Is(err, ErrNoAddresses) {
		t.Fatalf("newClient with empty addresses: err = %v, want %v", err, ErrNoAddresses)
	}
}

func TestBuildClientConfigDefaults(t *testing.T) {
	cfg := &Config{Addresses: []string{"hz-0:5701"}}

	hzCfg, err := buildClientConfig(cfg)
	if err != nil {
		t.Fatalf("buildClientConfig: %v", err)
	}

	if got, want := cfg.ClusterName, "dev"; got != want {
		t.Errorf("Config.ClusterName: got %q, want %q (defaults not applied to user config)", got, want)
	}
	if got, want := cfg.MapName, "souin-cache"; got != want {
		t.Errorf("Config.MapName: got %q, want %q (defaults not applied to user config)", got, want)
	}
	if got, want := hzCfg.Cluster.Name, "dev"; got != want {
		t.Errorf("hzCfg.Cluster.Name: got %q, want %q", got, want)
	}
	if !reflect.DeepEqual(hzCfg.Cluster.Network.Addresses, []string{"hz-0:5701"}) {
		t.Errorf("hzCfg.Cluster.Network.Addresses: got %v, want [hz-0:5701]", hzCfg.Cluster.Network.Addresses)
	}
	if got := hzCfg.Cluster.Security.Credentials.Username; got != "" {
		t.Errorf("hzCfg.Cluster.Security.Credentials.Username: got %q, want empty", got)
	}
}

func TestBuildClientConfigOverrides(t *testing.T) {
	cfg := &Config{
		Addresses:      []string{"hz-0:5701", "hz-1:5701"},
		ClusterName:    "prod-cache",
		MapName:        "responses",
		Username:       "alice",
		Password:       "secret",
		ConnectTimeout: 250 * time.Millisecond,
	}

	hzCfg, err := buildClientConfig(cfg)
	if err != nil {
		t.Fatalf("buildClientConfig: %v", err)
	}

	if got, want := hzCfg.Cluster.Name, "prod-cache"; got != want {
		t.Errorf("hzCfg.Cluster.Name: got %q, want %q", got, want)
	}
	if !reflect.DeepEqual(hzCfg.Cluster.Network.Addresses, []string{"hz-0:5701", "hz-1:5701"}) {
		t.Errorf("hzCfg.Cluster.Network.Addresses: got %v, want [hz-0:5701 hz-1:5701]", hzCfg.Cluster.Network.Addresses)
	}
	if got, want := hzCfg.Cluster.Security.Credentials.Username, "alice"; got != want {
		t.Errorf("Username: got %q, want %q", got, want)
	}
	if got, want := hzCfg.Cluster.Security.Credentials.Password, "secret"; got != want {
		t.Errorf("Password: got %q, want %q", got, want)
	}
	if got, want := hzCfg.Cluster.Network.ConnectionTimeout, types.Duration(250*time.Millisecond); got != want {
		t.Errorf("ConnectionTimeout: got %v, want %v", got, want)
	}
}

func TestBuildClientConfigSmartRouting(t *testing.T) {
	smartOn := true
	smartOff := false
	tests := []struct {
		name         string
		smartRouting *bool
		wantUnisock  bool
	}{
		{name: "unset keeps hazelcast default (smart routing on)", smartRouting: nil, wantUnisock: false},
		{name: "explicit true keeps smart routing on", smartRouting: &smartOn, wantUnisock: false},
		{name: "explicit false switches to unisocket", smartRouting: &smartOff, wantUnisock: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Addresses:    []string{"hz-0:5701"},
				SmartRouting: tt.smartRouting,
			}
			hzCfg, err := buildClientConfig(cfg)
			if err != nil {
				t.Fatalf("buildClientConfig: %v", err)
			}
			if got := hzCfg.Cluster.Unisocket; got != tt.wantUnisock {
				t.Errorf("Unisocket: got %v, want %v", got, tt.wantUnisock)
			}
		})
	}
}

func TestBuildClientConfigClonesAddresses(t *testing.T) {
	addrs := []string{"hz-0:5701"}
	cfg := &Config{Addresses: addrs}

	hzCfg, err := buildClientConfig(cfg)
	if err != nil {
		t.Fatalf("buildClientConfig: %v", err)
	}

	addrs[0] = "tampered:0"
	if got := hzCfg.Cluster.Network.Addresses[0]; got != "hz-0:5701" {
		t.Errorf("addresses not cloned: got %q after caller mutation, want hz-0:5701", got)
	}
}
