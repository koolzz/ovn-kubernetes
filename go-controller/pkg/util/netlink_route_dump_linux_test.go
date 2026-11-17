// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const routeDumpTable = 10001

func inRouteDumpNamespace(tb testing.TB, run func()) {
	tb.Helper()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	original, err := netns.Get()
	require.NoError(tb, err)
	defer func() { require.NoError(tb, original.Close()) }()
	isolated, err := netns.New()
	if err == unix.EPERM || err == unix.EACCES {
		tb.Skip("requires network namespace privileges")
	}
	require.NoError(tb, err)
	defer func() {
		require.NoError(tb, netns.Set(original))
		require.NoError(tb, isolated.Close())
	}()
	run()
}

func routeDumpLink(tb testing.TB) netlink.Link {
	tb.Helper()
	link := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "route-dump0"}}
	require.NoError(tb, netlink.LinkAdd(link))
	require.NoError(tb, netlink.LinkSetUp(link))
	for _, cidr := range []string{"192.0.2.1/24", "2001:db8:ffff::1/64"} {
		addr, err := netlink.ParseAddr(cidr)
		require.NoError(tb, err)
		addr.Flags = unix.IFA_F_NODAD
		require.NoError(tb, netlink.AddrAdd(link, addr))
	}
	return link
}

func addRouteDumpEntry(tb testing.TB, link netlink.Link, cidr, gateway string, table int, protocol netlink.RouteProtocol) {
	tb.Helper()
	_, dst, err := net.ParseCIDR(cidr)
	require.NoError(tb, err)
	require.NoError(tb, netlink.RouteAdd(&netlink.Route{LinkIndex: link.Attrs().Index, Dst: dst,
		Gw: net.ParseIP(gateway), Table: table, Protocol: protocol}))
}

func TestBGPRouteDumpFiltering(t *testing.T) {
	inRouteDumpNamespace(t, func() {
		// t.Run starts another goroutine, which would not share this thread's
		// network namespace. Keep the case checks on the namespace owner.
		runCase := func(name string, check func(*testing.T)) { t.Log(name); check(t) }
		link := routeDumpLink(t)
		for _, ipv6 := range []bool{false, true} {
			base, gateway := "10.0.%d.0/24", "192.0.2.2"
			if ipv6 {
				base, gateway = "2001:db8:%x::/64", "2001:db8:ffff::2"
			}
			addRouteDumpEntry(t, link, fmt.Sprintf(base, 1), gateway, routeDumpTable, unix.RTPROT_BGP)
			addRouteDumpEntry(t, link, fmt.Sprintf(base, 2), gateway, routeDumpTable, unix.RTPROT_STATIC)
			addRouteDumpEntry(t, link, fmt.Sprintf(base, 3), gateway, routeDumpTable+1, unix.RTPROT_BGP)
			// Multipath and non-unicast BGP entries must retain the original dump semantics.
			_, dst, err := net.ParseCIDR(fmt.Sprintf(base, 4))
			require.NoError(t, err)
			require.NoError(t, netlink.RouteAdd(&netlink.Route{Dst: dst, Table: routeDumpTable, Protocol: unix.RTPROT_BGP,
				MultiPath: []*netlink.NexthopInfo{{LinkIndex: link.Attrs().Index, Gw: net.ParseIP(gateway)}}}))
			_, dst, err = net.ParseCIDR(fmt.Sprintf(base, 5))
			require.NoError(t, err)
			require.NoError(t, netlink.RouteAdd(&netlink.Route{Dst: dst, Table: routeDumpTable,
				Protocol: unix.RTPROT_BGP, Type: unix.RTN_BLACKHOLE}))
		}
		ops := defaultNetLinkOps{}
		const mask = netlink.RT_FILTER_TABLE | netlink.RT_FILTER_PROTOCOL
		for _, family := range []int{netlink.FAMILY_V4, netlink.FAMILY_V6, netlink.FAMILY_ALL} {
			for _, table := range []int{routeDumpTable, routeDumpTable + 1, routeDumpTable + 99} {
				runCase(fmt.Sprintf("family=%d/table=%d", family, table), func(t *testing.T) {
					filter := &netlink.Route{Table: table, Protocol: unix.RTPROT_BGP}
					want, err := netlink.RouteListFiltered(family, filter, mask)
					require.NoError(t, err)
					wantCount := 0
					if table == routeDumpTable {
						wantCount = 3
					} else if table == routeDumpTable+1 {
						wantCount = 1
					}
					if family == netlink.FAMILY_ALL {
						wantCount *= 2
					}
					require.Len(t, want, wantCount, "the fixture must be read from its isolated namespace")
					var got []netlink.Route
					require.NoError(t, ops.RouteListFilteredIter(family, filter, mask, func(r netlink.Route) bool { got = append(got, r); return true }))
					require.ElementsMatch(t, want, got)
				})
			}
		}
		runCase("callback can stop early", func(t *testing.T) {
			seen := 0
			require.NoError(t, ops.RouteListFilteredIter(netlink.FAMILY_ALL, &netlink.Route{Table: routeDumpTable, Protocol: unix.RTPROT_BGP}, mask, func(netlink.Route) bool { seen++; return false }))
			require.Equal(t, 1, seen)
		})
		runCase("exact route lookup and other protocol remain compatible", func(t *testing.T) {
			_, dst, err := net.ParseCIDR("10.0.1.0/24")
			require.NoError(t, err)
			filter := &netlink.Route{Table: routeDumpTable, Dst: dst, LinkIndex: link.Attrs().Index, Gw: net.ParseIP("192.0.2.2")}
			exactMask := uint64(netlink.RT_FILTER_TABLE | netlink.RT_FILTER_DST | netlink.RT_FILTER_OIF | netlink.RT_FILTER_GW)
			want, err := netlink.RouteListFiltered(netlink.FAMILY_V4, filter, exactMask)
			require.NoError(t, err)
			require.Len(t, want, 1)
			var got []netlink.Route
			require.NoError(t, ops.RouteListFilteredIter(netlink.FAMILY_V4, filter, exactMask, func(r netlink.Route) bool { got = append(got, r); return true }))
			require.ElementsMatch(t, want, got)
			got, err = ops.RouteListFiltered(netlink.FAMILY_V4, filter, exactMask)
			require.NoError(t, err)
			require.ElementsMatch(t, want, got, "strict mode must not affect package/shared handles")
			filter = &netlink.Route{Table: routeDumpTable, Protocol: unix.RTPROT_STATIC}
			want, err = netlink.RouteListFiltered(netlink.FAMILY_ALL, filter, mask)
			require.NoError(t, err)
			got = nil
			require.NoError(t, ops.RouteListFilteredIter(netlink.FAMILY_ALL, filter, mask, func(r netlink.Route) bool { got = append(got, r); return true }))
			require.ElementsMatch(t, want, got)
		})
		runCase("request fields outside the mask keep legacy semantics", func(t *testing.T) {
			_, dst, err := net.ParseCIDR("10.0.1.0/24")
			require.NoError(t, err)
			filter := &netlink.Route{Table: routeDumpTable, Protocol: unix.RTPROT_BGP, Dst: dst}
			want, err := netlink.RouteListFiltered(netlink.FAMILY_V4, filter, mask)
			require.NoError(t, err)
			var got []netlink.Route
			require.NoError(t, ops.RouteListFilteredIter(netlink.FAMILY_V4, filter, mask, func(r netlink.Route) bool { got = append(got, r); return true }))
			require.ElementsMatch(t, want, got)
		})
		runCase("per-call sockets are closed", func(t *testing.T) {
			// Warm the process's runtime descriptors before checking for per-call leaks.
			filter := &netlink.Route{Table: routeDumpTable, Protocol: unix.RTPROT_BGP}
			require.NoError(t, ops.RouteListFilteredIter(netlink.FAMILY_ALL, filter, mask, func(netlink.Route) bool { return true }))
			before, err := os.ReadDir("/proc/self/fd")
			require.NoError(t, err)
			for range 30 {
				require.NoError(t, ops.RouteListFilteredIter(netlink.FAMILY_ALL, filter, mask, func(netlink.Route) bool { return true }))
				require.NoError(t, ops.RouteListFilteredIter(netlink.FAMILY_ALL, filter, mask, func(netlink.Route) bool { return false }))
			}
			after, err := os.ReadDir("/proc/self/fd")
			require.NoError(t, err)
			require.Len(t, after, len(before))
		})
	})
}

// Reject only NETLINK_GET_STRICT_CHK in a child process. This tests the real
// older-kernel fallback without adding a production handle factory or hook.
func TestBGPRouteDumpUnsupportedStrictOption(t *testing.T) {
	const childEnv = "OVNK_TEST_NO_STRICT_NETLINK"
	if os.Getenv(childEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.v", "-test.run=^TestBGPRouteDumpUnsupportedStrictOption$")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
		if strings.Contains(string(output), "requires network namespace privileges") || strings.Contains(string(output), "requires seccomp filter support") {
			t.Skip(string(output))
		}
		require.Contains(t, string(output), "strict fallback validated", string(output))
		return
	}
	inRouteDumpNamespace(t, func() {
		link := routeDumpLink(t)
		addRouteDumpEntry(t, link, "10.0.1.0/24", "192.0.2.2", routeDumpTable, unix.RTPROT_BGP)
		type seccompData struct {
			nr   int32
			arch uint32
			ip   uint64
			args [6]uint64
		}
		const arg1 = uint32(unsafe.Offsetof(seccompData{}.args) + unsafe.Sizeof(uint64(0)))
		const arg2 = arg1 + uint32(unsafe.Sizeof(uint64(0)))
		filter := []unix.SockFilter{
			{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
			{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: uint32(unix.SYS_SETSOCKOPT), Jf: 5},
			{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: arg1},
			{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.SOL_NETLINK, Jf: 3},
			{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: arg2},
			{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.NETLINK_GET_STRICT_CHK, Jf: 1},
			{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.ENOPROTOOPT)},
			{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
		}
		program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
		require.NoError(t, unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0))
		err := unix.Prctl(unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER, uintptr(unsafe.Pointer(&program)), 0, 0)
		if err == unix.EINVAL || err == unix.EACCES || err == unix.EPERM {
			t.Skip("requires seccomp filter support")
		}
		require.NoError(t, err)
		runtime.KeepAlive(&program)
		h, err := netlink.NewHandle(unix.NETLINK_ROUTE)
		require.NoError(t, err)
		require.ErrorIs(t, h.SetStrictCheck(true), unix.ENOPROTOOPT)
		require.NoError(t, h.Close())
		// Leave one descriptor available. A private strict handle must release
		// its socket before the non-strict retry opens another one.
		var originalLimit unix.Rlimit
		require.NoError(t, unix.Getrlimit(unix.RLIMIT_NOFILE, &originalLimit))
		limit := originalLimit
		if limit.Cur > 64 {
			limit.Cur = 64
		}
		require.NoError(t, unix.Setrlimit(unix.RLIMIT_NOFILE, &limit))
		var held []int
		defer func() {
			for _, fd := range held {
				require.NoError(t, unix.Close(fd))
			}
			require.NoError(t, unix.Setrlimit(unix.RLIMIT_NOFILE, &originalLimit))
		}()
		for {
			fd, openErr := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
			if openErr == unix.EMFILE {
				break
			}
			require.NoError(t, openErr)
			held = append(held, fd)
		}
		require.NotEmpty(t, held)
		require.NoError(t, unix.Close(held[len(held)-1]))
		held = held[:len(held)-1]
		count := 0
		require.NoError(t, (defaultNetLinkOps{}).RouteListFilteredIter(netlink.FAMILY_ALL,
			&netlink.Route{Table: routeDumpTable, Protocol: unix.RTPROT_BGP}, netlink.RT_FILTER_TABLE|netlink.RT_FILTER_PROTOCOL,
			func(netlink.Route) bool { count++; return true }))
		require.Equal(t, 1, count)
		t.Log("strict fallback validated")
	})
}

func BenchmarkBGPRouteDump(b *testing.B) {
	for _, mix := range []string{"many-tables", "mixed-protocols", "dense"} {
		for _, optimized := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/optimized=%t", mix, optimized), func(b *testing.B) {
				inRouteDumpNamespace(b, func() {
					link := routeDumpLink(b)
					for i := 0; i < 1000; i++ {
						table, protocol := routeDumpTable, netlink.RouteProtocol(unix.RTPROT_BGP)
						if mix == "many-tables" {
							table += i % 20
						} else if mix == "mixed-protocols" && i%20 != 0 {
							protocol = unix.RTPROT_STATIC
						}
						addRouteDumpEntry(b, link, fmt.Sprintf("10.%d.%d.0/24", i/256, i%256), "192.0.2.2", table, protocol)
					}
					dump := netlink.RouteListFilteredIter
					if optimized {
						dump = (defaultNetLinkOps{}).RouteListFilteredIter
					}
					filter := &netlink.Route{Table: routeDumpTable, Protocol: unix.RTPROT_BGP}
					wantCount := 50
					if mix == "dense" {
						wantCount = 1000
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						count := 0
						err := dump(netlink.FAMILY_ALL, filter, netlink.RT_FILTER_TABLE|netlink.RT_FILTER_PROTOCOL, func(netlink.Route) bool { count++; return true })
						if err != nil || count != wantCount {
							b.Fatalf("dump returned %d routes: %v", count, err)
						}
					}
					b.StopTimer()
				})
			})
		}
	}
}
