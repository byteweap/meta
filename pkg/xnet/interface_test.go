package xnet

import (
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInternalIPChoosesPreferredVersusFallbackAddress(t *testing.T) {
	interfaces := func() ([]net.Interface, error) {
		return []net.Interface{
			{Name: "loopback", Flags: net.FlagUp | net.FlagLoopback},
			{Name: "fallback", Flags: net.FlagUp},
			{Name: "preferred", Flags: net.FlagUp},
		}, nil
	}
	addresses := func(preferred bool) func(net.Interface) ([]net.Addr, error) {
		return func(iface net.Interface) ([]net.Addr, error) {
			switch iface.Name {
			case "fallback":
				return []net.Addr{
					&net.UnixAddr{Name: "invalid", Net: "unix"},
					&net.IPAddr{IP: net.ParseIP("127.0.0.1")},
					&net.IPAddr{IP: net.ParseIP("10.0.0.1")},
					&net.IPAddr{IP: net.ParseIP("10.0.0.2")},
				}, nil
			case "preferred":
				if preferred {
					return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.2")}}, nil
				}
				return nil, nil
			default:
				t.Fatalf("queried addresses for %q", iface.Name)
				return nil, nil
			}
		}
	}
	for preferred, want := range map[bool]string{true: "192.168.1.2", false: "10.0.0.1"} {
		got, err := internalIP(interfaces, addresses(preferred))
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestInternalIPHandlesMissingAddressesAndSystemErrors(t *testing.T) {
	failure := errors.New("interface lookup failed")
	for _, tt := range []struct {
		list    func() ([]net.Interface, error)
		addrs   func(net.Interface) ([]net.Addr, error)
		wantErr error
	}{
		{func() ([]net.Interface, error) { return nil, failure }, nil, failure},
		{func() ([]net.Interface, error) { return nil, nil }, nil, nil},
		{
			func() ([]net.Interface, error) { return []net.Interface{{Flags: net.FlagUp}}, nil },
			func(net.Interface) ([]net.Addr, error) { return nil, failure }, failure,
		},
	} {
		got, err := internalIP(tt.list, tt.addrs)
		require.Empty(t, got)
		if tt.wantErr != nil {
			require.ErrorIs(t, err, tt.wantErr)
		} else {
			require.EqualError(t, err, "not found ip address")
		}
	}
}
