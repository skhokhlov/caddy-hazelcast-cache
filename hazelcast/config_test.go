package hazelcast_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/skhokhlov/caddy-cache-hazelcast/hazelcast"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		in      hazelcast.Config
		wantErr error // matched via errors.Is when non-nil
		anyErr  bool  // accepts any non-nil error
		want    hazelcast.Config
	}{
		{
			name:    "empty addresses",
			in:      hazelcast.Config{},
			wantErr: hazelcast.ErrNoAddresses,
		},
		{
			name: "defaults applied",
			in: hazelcast.Config{
				Addresses: []string{"hz-0:5701"},
			},
			want: hazelcast.Config{
				Addresses:   []string{"hz-0:5701"},
				ClusterName: "dev",
				MapName:     "souin-cache",
			},
		},
		{
			name: "explicit cluster_name kept",
			in: hazelcast.Config{
				Addresses:   []string{"hz-0:5701"},
				ClusterName: "prod-cache",
			},
			want: hazelcast.Config{
				Addresses:   []string{"hz-0:5701"},
				ClusterName: "prod-cache",
				MapName:     "souin-cache",
			},
		},
		{
			name: "explicit map_name kept",
			in: hazelcast.Config{
				Addresses: []string{"hz-0:5701"},
				MapName:   "responses",
			},
			want: hazelcast.Config{
				Addresses:   []string{"hz-0:5701"},
				ClusterName: "dev",
				MapName:     "responses",
			},
		},
		{
			name: "tls cert without key rejected",
			in: hazelcast.Config{
				Addresses: []string{"hz-0:5701"},
				TLS: &hazelcast.TLSConfig{
					CertFile: "/etc/certs/client.pem",
				},
			},
			anyErr: true,
		},
		{
			name: "tls key without cert rejected",
			in: hazelcast.Config{
				Addresses: []string{"hz-0:5701"},
				TLS: &hazelcast.TLSConfig{
					KeyFile: "/etc/certs/client.key",
				},
			},
			anyErr: true,
		},
		{
			name: "valid mTLS",
			in: hazelcast.Config{
				Addresses: []string{"hz-0:5701"},
				TLS: &hazelcast.TLSConfig{
					CACertFile: "/etc/certs/ca.pem",
					CertFile:   "/etc/certs/client.pem",
					KeyFile:    "/etc/certs/client.key",
					ServerName: "hazelcast.internal",
				},
			},
			want: hazelcast.Config{
				Addresses:   []string{"hz-0:5701"},
				ClusterName: "dev",
				MapName:     "souin-cache",
				TLS: &hazelcast.TLSConfig{
					CACertFile: "/etc/certs/ca.pem",
					CertFile:   "/etc/certs/client.pem",
					KeyFile:    "/etc/certs/client.key",
					ServerName: "hazelcast.internal",
				},
			},
		},
		{
			name: "tls trust-only accepted",
			in: hazelcast.Config{
				Addresses: []string{"hz-0:5701"},
				TLS: &hazelcast.TLSConfig{
					CACertFile: "/etc/certs/ca.pem",
				},
			},
			want: hazelcast.Config{
				Addresses:   []string{"hz-0:5701"},
				ClusterName: "dev",
				MapName:     "souin-cache",
				TLS: &hazelcast.TLSConfig{
					CACertFile: "/etc/certs/ca.pem",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in
			err := got.Validate()
			switch {
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Validate() err = %v, want %v", err, tt.wantErr)
				}
				return
			case tt.anyErr:
				if err == nil {
					t.Fatalf("Validate() expected error, got nil")
				}
				return
			default:
				if err != nil {
					t.Fatalf("Validate() unexpected error: %v", err)
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Validate() got = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestConfigJSONRoundtrip(t *testing.T) {
	original := hazelcast.Config{
		Addresses:      []string{"hz-0:5701", "hz-1:5701"},
		ClusterName:    "prod-cache",
		MapName:        "responses",
		Username:       "user",
		Password:       "pass",
		ReadTimeout:    50 * time.Millisecond,
		WriteTimeout:   200 * time.Millisecond,
		ConnectTimeout: 5 * time.Second,
		SmartRouting:   true,
		TLS: &hazelcast.TLSConfig{
			CACertFile:         "/etc/certs/ca.pem",
			CertFile:           "/etc/certs/client.pem",
			KeyFile:            "/etc/certs/client.key",
			ServerName:         "hazelcast.internal",
			InsecureSkipVerify: false,
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var decoded hazelcast.Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf("roundtrip mismatch:\noriginal=%#v\ndecoded =%#v", original, decoded)
	}
}
