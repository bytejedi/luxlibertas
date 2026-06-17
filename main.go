package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var (
	version   = "v0.5.3"
	gitCommit = "unknown"
	buildTime = "unknown"
	goVersion = "unknown"
	platform  = "unknown"
)

// Config represents the simple JSON config format for our lightweight server.
type Config struct {
	Listen string `json:"listen"` // Address to listen on, e.g. "0.0.0.0:8080"
	Path   string `json:"path"`   // WebSocket path, e.g. "/vless"
	UUID   string `json:"uuid"`   // VLESS Client UUID, e.g. "de305d54-75b4-431b-adb2-eb6b9e546013"
}

const (
	// copyBufSize is the slab size used for TCP relay copies.
	// 16 KB is large enough to saturate a typical VPS uplink while
	// halving per-goroutine pool footprint vs. the old 32 KB value.
	copyBufSize = 16384

	// udpBufSize caps the per-relay UDP read buffer.
	// Real-world UDP-over-WS payloads are almost always < 4 KB;
	// 16 KB covers any realistic case without wasting 64 KB per relay.
	udpBufSize = 16384

	// muxBufSize is the slab size used by the Mux read/payload pools.
	// Aligned with copyBufSize (== upgrader.WriteBufferSize) so that a single
	// target read produces a single WebSocket frame and thus a single write
	// syscall. A 32 KB slab would overflow gorilla's 16 KB writeBuf and split
	// each downlink frame into ~2 syscalls, adding latency for no throughput
	// benefit. Keeping it at 16 KB also halves per-session RSS.
	muxBufSize = copyBufSize
)

var (
	config     Config
	clientUUID [16]byte
	// Upgrader buffer sizes control gorilla/websocket's internal I/O buffers.
	// 4 KB is sufficient: gorilla reads one WebSocket frame at a time and the
	// kernel socket buffer holds the rest. Large values inflate RSS at idle.
	upgrader = websocket.Upgrader{
		// ReadBufferSize: gorilla uses this only for WS frame-header parsing;
		// 4 KB is more than enough and keeps per-connection RSS low.
		ReadBufferSize: 4096,
		// WriteBufferSize MUST match copyBufSize.
		// gorilla flushes its internal writeBuf every time it fills; if
		// writeBuf (4 KB) < payload (16 KB), each relay Write triggers ~4
		// syscalls instead of 1.  Aligning them collapses that to 1 syscall.
		WriteBufferSize: copyBufSize,
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow connections from any origin
		},
	}
	dialer = &net.Dialer{
		Timeout:   time.Second * 10,
		KeepAlive: time.Second * 30,
	}
	copyBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, copyBufSize)
		},
	}
	// udpOutPool reuses header+payload slabs for the UDP downlink path,
	// eliminating a heap allocation on every received UDP packet.
	udpOutPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 2+udpBufSize)
			return &b
		},
	}
	// vlessRespHeader is the two-byte VLESS response header (version + addons length).
	// Declared at package level so it lives in the data segment: passing vlessRespHeader[:]
	// to an interface method is stack-cheap and never escapes to the heap, unlike
	// a []byte{0x00, 0x00} literal which the compiler cannot prove doesn't escape.
	vlessRespHeader = [2]byte{0x00, 0x00}
	// wsConnPool pools wsConn wrappers to minimize heap allocations during client handshakes.
	wsConnPool = sync.Pool{
		New: func() interface{} {
			return &wsConn{}
		},
	}

	// udpIdleTimeout bounds how long a UDP relay's read may block with no
	// traffic. UDP is connectionless, so a peer that never replies would
	// otherwise leak a goroutine + fd indefinitely. The deadline is refreshed
	// on every successful read/write, so it only fires on true idleness. It is
	// a var (not const) solely so tests can shrink it; production never mutates it.
	udpIdleTimeout = 60 * time.Second
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "version" {
		fmt.Printf("Version:    %s\n", version)
		fmt.Printf("Git Commit: %s\n", gitCommit)
		fmt.Printf("Build Time: %s\n", buildTime)
		fmt.Printf("Go Version: %s\n", goVersion)
		fmt.Printf("Platform:   %s\n", platform)
		return
	}

	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <config-file.json> | version\n", os.Args[0])
		return
	}

	// Configure slog default logger to write structured logs directly to os.Stdout
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// GCPercent=50: trigger GC when heap grows by 50% (default is 100%).
	// This is a middle ground — returns freed buffers to the OS faster than
	// the default without the extra GC cycles that a value of 20 would cause.
	debug.SetGCPercent(50)

	configFile := os.Args[1]
	data, err := os.ReadFile(configFile)
	if err != nil {
		slog.Error("Failed to read config file", "error", err)
		os.Exit(1)
	}

	if err := json.Unmarshal(data, &config); err != nil {
		slog.Error("Failed to parse config JSON", "error", err)
		os.Exit(1)
	}

	if config.Listen == "" || config.Path == "" || config.UUID == "" {
		slog.Error("Config fields 'listen', 'path', and 'uuid' are required.")
		os.Exit(1)
	}

	// Normalise and parse UUID
	parsedUUID, err := parseUUID(config.UUID)
	if err != nil {
		slog.Error("Invalid VLESS UUID", "error", err)
		os.Exit(1)
	}
	clientUUID = parsedUUID

	// Build a dedicated ServeMux instead of using http.DefaultServeMux.
	// DefaultServeMux is a global, shared across all packages; a dedicated
	// mux ensures no accidental handler leakage from imported packages.
	httpMux := http.NewServeMux()
	httpMux.HandleFunc(config.Path, handleVLESS)

	// Configure the HTTP server explicitly rather than using the bare
	// http.ListenAndServe helper, which uses zero-value (insecure) defaults:
	//
	//   ReadHeaderTimeout — without this, a slow-loris client can hold a
	//     goroutine open indefinitely by drip-feeding the HTTP headers.
	//
	//   MaxHeaderBytes — a WebSocket upgrade request needs only ~200 bytes
	//     of headers; capping at 4 KB (vs the default 1 MB) prevents
	//     oversized header attacks and reduces peak RSS during the handshake.
	srv := &http.Server{
		Addr:              config.Listen,
		Handler:           httpMux,
		ReadHeaderTimeout: 10 * time.Second,
		MaxHeaderBytes:    4 << 10, // 4 KB
	}

	slog.Info("Starting LuxLibertas server (Mux Enabled)")

	if err := srv.ListenAndServe(); err != nil {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}

// wsConn wraps a Gorilla WebSocket connection to implement the io.ReadWriteCloser interface.
type wsConn struct {
	*websocket.Conn
	reader io.Reader
	// br is a bufio.Reader bound to this wsConn, carried on the struct so it is
	// pooled together with the wsConn wrapper (via wsConnPool). Previously each
	// connection allocated a fresh 512-byte bufio.Reader in handleVLESS; reusing
	// it across connections via bufio.Reader.Reset removes that per-connection
	// allocation under high handshake churn. Lazily created on first use.
	br *bufio.Reader
}

func (c *wsConn) Read(b []byte) (int, error) {
	for {
		if c.reader != nil {
			n, err := c.reader.Read(b)
			if err == io.EOF {
				c.reader = nil
				if n > 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}
		_, reader, err := c.NextReader()
		if err != nil {
			return 0, err
		}
		c.reader = reader
	}
}

func (c *wsConn) Write(b []byte) (int, error) {
	err := c.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func parseUUID(s string) ([16]byte, error) {
	var uuid [16]byte
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return uuid, fmt.Errorf("UUID must be 36 characters (or 32 hex digits)")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return uuid, err
	}
	copy(uuid[:], b)
	return uuid, nil
}

// parseVLESSHeader decodes the standard VLESS request header and returns the targetAddr directly.
func parseVLESSHeader(br *bufio.Reader, expectedUUID [16]byte) (string, byte, error) {
	// The VLESS header starts with: Version (1 byte) + UUID (16 bytes) + Addons Length (1 byte) = 18 bytes.
	// Peek 18 bytes directly from the bufio.Reader's internal buffer with zero-allocation.
	fixedHeader, err := br.Peek(18)
	if err != nil {
		return "", 0, fmt.Errorf("failed to peek fixed header prefix: %w", err)
	}

	if fixedHeader[0] != 0 {
		return "", 0, fmt.Errorf("unsupported version: %d", fixedHeader[0])
	}

	var uuidBytes [16]byte
	copy(uuidBytes[:], fixedHeader[1:17])
	if uuidBytes != expectedUUID {
		return "", 0, fmt.Errorf("unauthorised client UUID")
	}

	addonsLen := int(fixedHeader[17])
	// We have peeked 18 bytes. We must now discard them to advance.
	if _, err := br.Discard(18); err != nil {
		return "", 0, err
	}

	if addonsLen > 0 {
		if _, err := br.Discard(addonsLen); err != nil {
			return "", 0, fmt.Errorf("failed to discard addons content: %w", err)
		}
	}

	// Peek Command (1 byte).
	cmdBytes, err := br.Peek(1)
	if err != nil {
		return "", 0, fmt.Errorf("failed to peek command: %w", err)
	}

	cmd := cmdBytes[0]
	if _, err := br.Discard(1); err != nil {
		return "", 0, err
	}

	// Mux (cmd == 3) carries no destination port/address in the VLESS header.
	if cmd == 3 {
		return "", cmd, nil
	}

	// Peek Port (2 bytes) + Address Type (1 byte) = 3 bytes.
	portAddrType, err := br.Peek(3)
	if err != nil {
		return "", 0, fmt.Errorf("failed to peek port and address type: %w", err)
	}

	port := int(portAddrType[0])<<8 | int(portAddrType[1])
	addrType := portAddrType[2]

	if _, err := br.Discard(3); err != nil {
		return "", 0, err
	}

	var targetAddr string
	switch addrType {
	case 1: // IPv4 (4 bytes)
		ipBytes, err := br.Peek(4)
		if err != nil {
			return "", 0, fmt.Errorf("failed to peek IPv4 address: %w", err)
		}
		var ip [4]byte
		copy(ip[:], ipBytes)
		if _, err := br.Discard(4); err != nil {
			return "", 0, err
		}
		// Convert directly to netip.AddrPort in a single string allocation
		targetAddr = netip.AddrPortFrom(netip.AddrFrom4(ip), uint16(port)).String()

	case 2: // Domain (1 byte length + length bytes string)
		domainLenBytes, err := br.Peek(1)
		if err != nil {
			return "", 0, fmt.Errorf("failed to peek domain length: %w", err)
		}
		domainLen := int(domainLenBytes[0])
		if _, err := br.Discard(1); err != nil {
			return "", 0, err
		}
		domainBytes, err := br.Peek(domainLen)
		if err != nil {
			return "", 0, fmt.Errorf("failed to peek domain string: %w", err)
		}
		address := string(domainBytes)
		if _, err := br.Discard(domainLen); err != nil {
			return "", 0, err
		}
		// Standard JoinHostPort formatting
		targetAddr = net.JoinHostPort(address, strconv.Itoa(port))

	case 3: // IPv6 (16 bytes)
		ipBytes, err := br.Peek(16)
		if err != nil {
			return "", 0, fmt.Errorf("failed to peek IPv6 address: %w", err)
		}
		var ip [16]byte
		copy(ip[:], ipBytes)
		if _, err := br.Discard(16); err != nil {
			return "", 0, err
		}
		// Convert directly to netip.AddrPort in a single string allocation
		targetAddr = netip.AddrPortFrom(netip.AddrFrom16(ip), uint16(port)).String()

	default:
		return "", 0, fmt.Errorf("unsupported address type: %d", addrType)
	}

	return targetAddr, cmd, nil
}

func handleVLESS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Upgrade failed", "error", err)
		return
	}
	defer conn.Close()

	// Retrieve a pooled wsConn instance to reduce GC heap-alloc pressure.
	ws := wsConnPool.Get().(*wsConn)
	ws.Conn = conn
	ws.reader = nil

	// Defer returning the wsConn wrapper back to the pool, resetting its inner references.
	defer func() {
		ws.Conn = nil
		ws.reader = nil
		// Drop the bufio.Reader's reference to ws so the wrapper doesn't keep a
		// stale *websocket.Conn alive through br; the buffer itself stays
		// allocated for the next connection to reuse.
		if ws.br != nil {
			ws.br.Reset(nil)
		}
		wsConnPool.Put(ws)
	}()

	// Wrap ws in a bufio.Reader for VLESS header parsing. Reuse the pooled
	// reader carried on ws when present; otherwise create it once.
	var br *bufio.Reader
	if ws.br != nil {
		ws.br.Reset(ws)
		br = ws.br
	} else {
		br = bufio.NewReaderSize(ws, 512)
		ws.br = br
	}

	// 1. Decode VLESS Header
	targetAddr, cmd, err := parseVLESSHeader(br, clientUUID)
	if err != nil {
		slog.Error("Failed to parse VLESS header", "error", err)
		return
	}

	// 2. Handle Mux Command (0x03)
	if cmd == 3 {
		slog.Info("Multiplexed Connection (Mux)")

		// Write VLESS response header (version=0x00, addons length=0x00).
		if _, err := ws.Write(vlessRespHeader[:]); err != nil {
			slog.Error("Failed to write response header", "error", err)
			return
		}

		// Run our new, high-performance standalone Mux Server
		srv := NewMuxServer(dialer)
		if err := srv.Serve(br, ws); err != nil && err != io.EOF {
			slog.Error("Mux Server error", "error", err)
		}
		slog.Info("Mux Connection Closed")
		return
	}

	slog.Info("TCP/UDP Connection target", "target", targetAddr, "command", cmd)

	// 3. Dial target (Freedom Outbound)
	var dest net.Conn
	if cmd == 1 { // TCP
		dest, err = dialer.Dial("tcp", targetAddr)
		if err == nil {
			if tcpConn, ok := dest.(*net.TCPConn); ok {
				tcpConn.SetNoDelay(true)
			}
		}
	} else if cmd == 2 { // UDP
		dest, err = dialer.Dial("udp", targetAddr)
	} else {
		slog.Error("Unsupported VLESS command", "command", cmd)
		return
	}

	if err != nil {
		slog.Error("Failed to connect to target", "target", targetAddr, "error", err)
		return
	}
	defer dest.Close()

	// 4. Write VLESS Response Header (Version 0x00 + Addons Length 0x00)
	if _, err := ws.Write(vlessRespHeader[:]); err != nil {
		slog.Error("Failed to write response header", "error", err)
		return
	}

	// 5. Relay bidirectional traffic.
	if cmd == 1 {
		relayTCP(br, ws, dest)
	} else {
		relayUDP(br, ws, dest)
	}

	slog.Info("VLESS Connection Closed", "target", targetAddr)
}

// relayTCP copies data bidirectionally between the WebSocket and the target.
func relayTCP(wsReader io.Reader, wsWriter io.Writer, dest net.Conn) {
	errChan := make(chan error, 2)
	go copyBridge(dest, wsReader, dest, errChan) // client → target
	go copyBridge(wsWriter, dest, dest, errChan) // target → client
	<-errChan
	<-errChan // wait for both goroutines
}

// relayUDP copies framed UDP datagrams bidirectionally between the WebSocket and the target.
func relayUDP(wsReader io.Reader, wsWriter io.Writer, dest net.Conn) {
	errChan := make(chan error, 2)
	go relayUDPUplink(wsReader, dest, errChan)   // client → target (UDP)
	go relayUDPDownlink(wsWriter, dest, errChan) // target → client (UDP)
	<-errChan
	<-errChan // wait for both goroutines
}

// copyBridge performs zero-closure-allocation copy between Reader and Writer.
func copyBridge(dst io.Writer, src io.Reader, conn net.Conn, errChan chan<- error) {
	buf := copyBufPool.Get().([]byte)
	defer copyBufPool.Put(buf)
	_, err := io.CopyBuffer(dst, src, buf)
	_ = conn.Close() // unblock peer direction
	errChan <- err
}

// relayUDPUplink copies framed UDP packets from WS to target using a pooled buffer.
func relayUDPUplink(wsReader io.Reader, dest net.Conn, errChan chan<- error) {
	defer dest.Close()
	var lenBuf [2]byte
	pooledBuf := copyBufPool.Get().([]byte)
	defer copyBufPool.Put(pooledBuf)

	var err error
	for {
		if _, err = io.ReadFull(wsReader, lenBuf[:]); err != nil {
			break
		}
		packetLen := int(lenBuf[0])<<8 | int(lenBuf[1])
		// Use the pooled buffer for the common case; only oversized packets
		// (> copyBufSize) get a one-shot dynamic allocation. The dynamic
		// buffer is intentionally NOT returned to the pool — putting a
		// non-copyBufSize buffer back would pollute the pool's size invariant.
		packetBuf := pooledBuf
		if packetLen > len(pooledBuf) {
			packetBuf = make([]byte, packetLen)
		}
		if _, err = io.ReadFull(wsReader, packetBuf[:packetLen]); err != nil {
			break
		}
		if _, err = dest.Write(packetBuf[:packetLen]); err != nil {
			break
		}
		// Client traffic counts as activity: push the downlink's idle read
		// deadline forward so an active-uplink/silent-downlink session (e.g.
		// a one-way upstream) is not torn down by the idle timeout.
		_ = dest.SetReadDeadline(time.Now().Add(udpIdleTimeout))
	}
	errChan <- err
}

// relayUDPDownlink copies UDP packets from target to WS using a pooled slab.
func relayUDPDownlink(wsWriter io.Writer, dest net.Conn, errChan chan<- error) {
	defer dest.Close()
	slabPtr := udpOutPool.Get().(*[]byte)
	slab := *slabPtr
	defer udpOutPool.Put(slabPtr)

	readBuf := slab[2:] // payload portion; slab[0:2] holds the length header
	var err error
	for {
		var n int
		// Refresh the idle deadline before each read. UDP is connectionless,
		// so without this a peer that goes silent would pin this goroutine
		// and its fd forever. The deadline only fires on genuine idleness
		// since every successful packet pushes it forward.
		_ = dest.SetReadDeadline(time.Now().Add(udpIdleTimeout))
		if n, err = dest.Read(readBuf); err != nil {
			break
		}
		slab[0] = byte(n >> 8)
		slab[1] = byte(n)
		if _, err = wsWriter.Write(slab[:2+n]); err != nil {
			break
		}
	}
	errChan <- err
}

// ============================================================================
// High-Performance Standalone VLESS Mux Protocol Implementation
// ============================================================================

// Mux Session statuses
const (
	muxStatusNew       = 0x01
	muxStatusKeep      = 0x02
	muxStatusEnd       = 0x03
	muxStatusKeepAlive = 0x04
)

// Mux Options
const (
	muxOptData = 0x01
)

// muxSessionChanCap bounds the per-sub-stream payload queue. It exists purely
// for backpressure: when the target connection can't keep up, a full channel
// trips the non-blocking send's default branch and the slow session is torn
// down (head-of-line-blocking mitigation — see the muxStatusKeep handler).
// 32 slots is ample headroom for that purpose; the old value of 128 just
// pinned ~4 KB of channel buffer per sub-stream (muxPayload is 32 B) for no
// throughput benefit, since a healthy session drains far faster than 32 frames
// can accumulate. Lowering it cuts per-connection memory under many concurrent
// sub-streams without changing latency or the HoL semantics.
const muxSessionChanCap = 32

// MuxDialer wraps standard net.Dialer or custom dials.
type MuxDialer interface {
	Dial(network, address string) (net.Conn, error)
}

type muxPayload struct {
	ptr  *[]byte
	data []byte
}

type muxWriteFrame struct {
	bufPtr *[]byte
	data   []byte
}

// muxBufPool and muxPayloadPool are package-level pools shared across ALL Mux
// connections. They were previously per-MuxServer fields, which meant every WS
// connection allocated two fresh sync.Pools and discarded their cached 16 KB
// slabs when the connection (and thus the server) was GC'd — defeating the
// whole point of pooling under high connection churn. Hoisting them to package
// scope lets slabs survive connection teardown and be reused by the next
// connection. The buffers carry no per-connection state, so sharing is safe.
//
// Both are aligned with the WebSocket write buffer (copyBufSize) so a full
// downlink frame flushes in one syscall. See muxBufSize.
var (
	muxBufPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, muxBufSize)
			return &b
		},
	}
	muxPayloadPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, muxBufSize)
			return &b
		},
	}
)

// MuxServer handles V2Ray Mux wire protocol specifically for VLESS over WebSocket.
type MuxServer struct {
	dialer   MuxDialer
	writer   io.Writer
	sessions map[uint16]*MuxSession
	mu       sync.RWMutex
	writeMu  sync.Mutex
	writeCh  chan muxWriteFrame
	closed   bool
	// transport is the underlying WS connection. writeLoop closes it the moment
	// a downlink Write fails (client gone): this unblocks the Serve read loop
	// (blocked in io.ReadFull on the same connection) so the whole session tears
	// down at once instead of every sub-stream's readLoop continuing to pull
	// from its target, frame the data, and have it silently discarded by a dead
	// writeLoop. Set once in Serve before any goroutine runs; closed at most
	// once via closeTransport.
	transport io.Closer
	closeOnce sync.Once
}

// MuxSession represents a single multiplexed logical stream.
type MuxSession struct {
	id     uint16
	conn   net.Conn
	ch     chan muxPayload
	closed chan struct{}
	once   sync.Once
	server *MuxServer
	// isUDP marks UDP sub-streams. Their target read loop is connectionless
	// and would otherwise block forever on a silent peer, leaking a goroutine
	// + fd for the lifetime of the parent WS connection. UDP reads therefore
	// carry an idle deadline; TCP sub-streams never set one to avoid tearing
	// down legitimately long-lived idle connections (SSH, long-poll, etc.).
	isUDP bool
}

// NewMuxServer creates a new Mux Server instance.
func NewMuxServer(dialer MuxDialer) *MuxServer {
	return &MuxServer{
		dialer:   dialer,
		sessions: make(map[uint16]*MuxSession),
	}
}

// Serve handles parsing and routing incoming Mux frames from reader to target connections.
func (s *MuxServer) Serve(r io.Reader, w io.Writer) error {
	s.writer = w
	s.writeCh = make(chan muxWriteFrame, 1024)
	s.closed = false
	// Record the transport so writeLoop can unblock this read loop on a downlink
	// write failure. In production w is the *wsConn, whose Close terminates the
	// underlying WebSocket and thus unblocks io.ReadFull(r) below (r wraps the
	// same connection). If w is not a Closer (some tests pass a bare Writer),
	// transport stays nil and the behavior falls back to the prior teardown path.
	if c, ok := w.(io.Closer); ok {
		s.transport = c
	}

	writeLoopDone := make(chan struct{})
	go func() {
		s.writeLoop()
		close(writeLoopDone)
	}()

	defer func() {
		s.writeMu.Lock()
		s.closed = true
		close(s.writeCh)
		s.writeMu.Unlock()

		<-writeLoopDone
		s.CloseAll()
	}()

	for {
		// 1. Read MetaLen (2 bytes)
		var metaLenBuf [2]byte
		if _, err := io.ReadFull(r, metaLenBuf[:]); err != nil {
			return err
		}
		metaLen := binary.BigEndian.Uint16(metaLenBuf[:])
		if metaLen > 2048 {
			return fmt.Errorf("invalid metalen: %d", metaLen)
		}

		// 2. Read MetaData with 0-Alloc Stack Buffer Optimization
		var localMeta [256]byte
		var metaBytes []byte
		if metaLen <= 256 {
			metaBytes = localMeta[:metaLen]
		} else {
			metaBytes = make([]byte, metaLen)
		}
		if _, err := io.ReadFull(r, metaBytes); err != nil {
			return err
		}

		if len(metaBytes) < 4 {
			return fmt.Errorf("insufficient metadata: %d", len(metaBytes))
		}

		sessionID := binary.BigEndian.Uint16(metaBytes[0:2])
		status := metaBytes[2]
		option := metaBytes[3]

		var network string
		var targetAddr string
		var addrErr error

		if status == muxStatusNew {
			if len(metaBytes) < 8 {
				return fmt.Errorf("insufficient metadata for StatusNew: %d", len(metaBytes))
			}
			netType := metaBytes[4]
			if netType == 1 {
				network = "tcp"
			} else {
				network = "udp"
			}
			port := binary.BigEndian.Uint16(metaBytes[5:7])
			addrType := metaBytes[7]

			addrBytes := metaBytes[8:]
			var address string
			switch addrType {
			case 1: // IPv4
				if len(addrBytes) >= 4 {
					address = net.IP(addrBytes[:4]).String()
				} else {
					addrErr = fmt.Errorf("invalid ipv4 length")
				}
			case 2: // Domain
				if len(addrBytes) >= 1 {
					domLen := int(addrBytes[0])
					if len(addrBytes) >= 1+domLen {
						address = string(addrBytes[1 : 1+domLen])
					} else {
						addrErr = fmt.Errorf("invalid domain length: expected %d, got %d", domLen, len(addrBytes)-1)
					}
				} else {
					addrErr = fmt.Errorf("invalid domain prefix")
				}
			case 3: // IPv6
				if len(addrBytes) >= 16 {
					address = net.IP(addrBytes[:16]).String()
				} else {
					addrErr = fmt.Errorf("invalid ipv6 length")
				}
			default:
				addrErr = fmt.Errorf("unsupported address type: %d", addrType)
			}

			if addrErr == nil {
				targetAddr = net.JoinHostPort(address, strconv.Itoa(int(port)))
			}
		}

		// 3. Read Data Payload if Option has OptData with payloadPool optimization.
		// dataLen is a uint16 (max 65535), but the pooled buffer is only 32 KB.
		// A client sending dataLen > len(buf) would otherwise slice out of range
		// and panic. For the common case (<= pool size) reuse the pooled buffer;
		// oversized payloads get a one-shot allocation that is NOT pooled (to
		// preserve the pool's size invariant), signalled by leaving payloadPtr nil.
		var payloadPtr *[]byte
		var dataPayload []byte
		if option&muxOptData == muxOptData {
			var dataLenBuf [2]byte
			if _, err := io.ReadFull(r, dataLenBuf[:]); err != nil {
				return err
			}
			dataLen := int(binary.BigEndian.Uint16(dataLenBuf[:]))

			ptr := muxPayloadPool.Get().(*[]byte)
			if dataLen <= len(*ptr) {
				payloadPtr = ptr
				dataPayload = (*ptr)[:dataLen]
			} else {
				// Oversized: return the pooled buffer immediately, allocate
				// a dedicated slab, and leave payloadPtr nil so downstream
				// code never tries to recycle it.
				muxPayloadPool.Put(ptr)
				dataPayload = make([]byte, dataLen)
			}
			if _, err := io.ReadFull(r, dataPayload); err != nil {
				if payloadPtr != nil {
					muxPayloadPool.Put(payloadPtr)
				}
				return err
			}
		}

		// Process parsed Mux frame
		switch status {
		case muxStatusNew:
			if addrErr != nil {
				if payloadPtr != nil {
					muxPayloadPool.Put(payloadPtr)
				}
				s.sendEnd(sessionID)
				continue
			}

			conn, err := s.dialer.Dial(network, targetAddr)
			if err != nil {
				if payloadPtr != nil {
					muxPayloadPool.Put(payloadPtr)
				}
				s.sendEnd(sessionID)
				continue
			}
			// Disable Nagle on TCP sub-streams. Mux is the primary path for
			// these clients, and Nagle would coalesce small interactive packets
			// (SSH, HTTP request headers, game traffic) for ~40ms, adding
			// latency for no benefit on an already-buffered relay. Mirrors the
			// bare TCP path in handleVLESS.
			if network == "tcp" {
				if tcpConn, ok := conn.(*net.TCPConn); ok {
					tcpConn.SetNoDelay(true)
				}
			}

			sess := &MuxSession{
				id:     sessionID,
				conn:   conn,
				ch:     make(chan muxPayload, muxSessionChanCap),
				closed: make(chan struct{}),
				server: s,
				isUDP:  network == "udp",
			}

			s.addSession(sess)
			sess.startWriteLoop()
			sess.startReadLoop()

			if len(dataPayload) > 0 {
				select {
				case sess.ch <- muxPayload{ptr: payloadPtr, data: dataPayload}:
				default:
					if payloadPtr != nil {
						muxPayloadPool.Put(payloadPtr)
					}
					sess.Close()
				}
			}

		case muxStatusKeep:
			sess := s.getSession(sessionID)
			if sess != nil && len(dataPayload) > 0 {
				select {
				case sess.ch <- muxPayload{ptr: payloadPtr, data: dataPayload}:
				case <-sess.closed:
					if payloadPtr != nil {
						muxPayloadPool.Put(payloadPtr)
					}
				default:
					if payloadPtr != nil {
						muxPayloadPool.Put(payloadPtr)
					}
					// Head-of-line blocking mitigation: slow target connection is terminated
					sess.Close()
				}
			} else {
				if payloadPtr != nil {
					muxPayloadPool.Put(payloadPtr)
				}
				if sess == nil {
					s.sendEnd(sessionID)
				}
			}

		case muxStatusEnd:
			if payloadPtr != nil {
				muxPayloadPool.Put(payloadPtr)
			}
			sess := s.getSession(sessionID)
			if sess != nil {
				sess.closeLocal()
			}

		case muxStatusKeepAlive:
			if payloadPtr != nil {
				muxPayloadPool.Put(payloadPtr)
			}
			// Heartbeat, no-op
		}
	}
}

// CloseAll closes all active sessions cleanly.
func (s *MuxServer) CloseAll() {
	s.mu.Lock()
	sessions := s.sessions
	s.sessions = make(map[uint16]*MuxSession)
	s.mu.Unlock()

	for _, sess := range sessions {
		sess.closeLocal()
	}
}

func (s *MuxServer) addSession(sess *MuxSession) {
	s.mu.Lock()
	old := s.sessions[sess.id]
	s.sessions[sess.id] = sess
	s.mu.Unlock()

	// Defensive: a well-behaved client never reuses a sessionID before sending
	// StatusEnd for the prior session, but a buggy or hostile one might. Without
	// this, the old session's goroutines (read/write loops) would be orphaned —
	// nothing left in the map to close them, and a TCP sub-stream has no idle
	// timeout — leaking a goroutine + fd for the life of the WS connection.
	// Close the displaced session locally (the map already points at the new
	// one, so closeLocal's compare-and-delete is a no-op and won't evict it).
	if old != nil && old != sess {
		old.closeLocal()
	}
}

// removeSession deletes id from the session map ONLY if it still maps to sess.
// This compare-and-delete guards against an ABA race: sessionIDs may be reused,
// so between a closing session's teardown and its removeSession call, the client
// can open a NEW session with the same id. An unconditional delete would then
// evict that live new session, leaking its goroutines and causing its frames to
// be dropped as "unknown session". Comparing the pointer ensures a session only
// ever removes itself.
func (s *MuxServer) removeSession(id uint16, sess *MuxSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions[id] == sess {
		delete(s.sessions, id)
	}
}

func (s *MuxServer) getSession(id uint16) *MuxSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[id]
}

func (s *MuxServer) writeLoop() {
	var failed bool
	for frame := range s.writeCh {
		if !failed {
			if _, err := s.writer.Write(frame.data); err != nil {
				// Downlink is dead (client gone). Close the transport to unblock
				// the Serve read loop so the whole session tears down promptly,
				// then switch to drain-only mode: keep ranging so every queued
				// frame's buffer is still recycled (and the channel can be closed
				// cleanly by Serve's defer), but skip the doomed writes.
				failed = true
				s.closeTransport()
			}
		}
		if frame.bufPtr != nil {
			muxBufPool.Put(frame.bufPtr)
		}
	}
}

// closeTransport closes the underlying WS connection exactly once. Idempotent so
// it is safe to call from both writeLoop (on write failure) and any future
// caller without double-close races.
func (s *MuxServer) closeTransport() {
	s.closeOnce.Do(func() {
		if s.transport != nil {
			_ = s.transport.Close()
		}
	})
}

func (s *MuxServer) writeFrame(frame muxWriteFrame) {
	s.writeMu.Lock()
	if s.closed {
		s.writeMu.Unlock()
		if frame.bufPtr != nil {
			muxBufPool.Put(frame.bufPtr)
		}
		return
	}
	s.writeCh <- frame
	s.writeMu.Unlock()
}

func (s *MuxServer) sendEnd(sessionID uint16) {
	bufPtr := muxBufPool.Get().(*[]byte)
	buf := *bufPtr
	binary.BigEndian.PutUint16(buf[0:2], 4)
	binary.BigEndian.PutUint16(buf[2:4], sessionID)
	buf[4] = muxStatusEnd
	buf[5] = 0

	s.writeFrame(muxWriteFrame{
		bufPtr: bufPtr,
		data:   buf[:6],
	})
}

// startWriteLoop runs in a background goroutine and writes queued data chunks to target connection.
func (s *MuxSession) startWriteLoop() {
	go func() {
		for {
			select {
			case payload, ok := <-s.ch:
				if !ok {
					return
				}
				_, err := s.conn.Write(payload.data)
				if payload.ptr != nil {
					muxPayloadPool.Put(payload.ptr)
				}
				if err != nil {
					s.Close()
					return
				}
				// For UDP, uplink traffic counts as session activity: push the
				// downlink read deadline forward so a busy-uplink/silent-downlink
				// flow is not torn down by the idle timeout. Mirrors the bare-UDP
				// relay. SetReadDeadline is safe to call from another goroutine.
				if s.isUDP {
					_ = s.conn.SetReadDeadline(time.Now().Add(udpIdleTimeout))
				}
			case <-s.closed:
				return
			}
		}
	}()
}

// startReadLoop runs in a background goroutine, reading from target connection and writing back Mux frames.
func (s *MuxSession) startReadLoop() {
	go func() {
		for {
			bufPtr := muxBufPool.Get().(*[]byte)
			buf := *bufPtr

			// UDP sub-streams refresh an idle read deadline before every read
			// so a silent connectionless peer cannot pin this goroutine + fd
			// for the lifetime of the parent WS connection. TCP sub-streams
			// never set a deadline (zero-cost on the hot path) and rely on the
			// peer's FIN/RST, preserving long-lived idle connections.
			if s.isUDP {
				_ = s.conn.SetReadDeadline(time.Now().Add(udpIdleTimeout))
			}

			// Read directly starting from offset 8
			n, err := s.conn.Read(buf[8:])
			if n > 0 {
				// Construct header in-place: MetaLen (2), SessionID (2), StatusKeep (1), OptData (1), DataLen (2)
				binary.BigEndian.PutUint16(buf[0:2], 4)
				binary.BigEndian.PutUint16(buf[2:4], s.id)
				buf[4] = muxStatusKeep
				buf[5] = muxOptData
				binary.BigEndian.PutUint16(buf[6:8], uint16(n))

				s.server.writeFrame(muxWriteFrame{
					bufPtr: bufPtr,
					data:   buf[:8+n],
				})
			} else {
				muxBufPool.Put(bufPtr)
			}

			if err != nil {
				s.Close()
				return
			}
		}
	}()
}

// Close closes the session and sends a StatusEnd frame to client.
func (s *MuxSession) Close() {
	s.once.Do(func() {
		close(s.closed)
		if s.conn != nil {
			s.conn.Close()
		}
		s.server.removeSession(s.id, s)
		s.server.sendEnd(s.id)

		// Drain and reclaim buffers
		for {
			select {
			case payload := <-s.ch:
				if payload.ptr != nil {
					muxPayloadPool.Put(payload.ptr)
				}
			default:
				return
			}
		}
	})
}

// closeLocal closes the session locally without sending a StatusEnd frame to client.
func (s *MuxSession) closeLocal() {
	s.once.Do(func() {
		close(s.closed)
		if s.conn != nil {
			s.conn.Close()
		}
		s.server.removeSession(s.id, s)

		// Drain and reclaim buffers
		for {
			select {
			case payload := <-s.ch:
				if payload.ptr != nil {
					muxPayloadPool.Put(payload.ptr)
				}
			default:
				return
			}
		}
	})
}
