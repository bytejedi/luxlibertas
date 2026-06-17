package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// BenchmarkParseVLESSHeader measures the performance of the parseVLESSHeader function
// using a bufio.Reader as optimized in the code.
func BenchmarkParseVLESSHeader(b *testing.B) {
	// Construct a valid VLESS header:
	// 1. Version: 0x00 (1 byte)
	// 2. UUID: 16 bytes
	// 3. Addons: 0x00 (1 byte length)
	// 4. Command: 0x01 (1 byte, TCP)
	// 5. Port: 443 (2 bytes: 0x01, 0xBB)
	// 6. Address Type: 0x01 (1 byte, IPv4)
	// 7. IP Address: 127.0.0.1 (4 bytes: 127, 0, 0, 1)
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	headerBytes := []byte{
		0x00,                                                  // Version
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		0x00,       // Addons length
		0x01,       // Command (TCP)
		0x01, 0xBB, // Port (443)
		0x01,         // Addr Type (IPv4)
		127, 0, 0, 1, // IP
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(headerBytes)
		br := bufio.NewReaderSize(r, 512)
		_, _, err := parseVLESSHeader(br, uuid)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseVLESSHeader_Pure measures the performance of the parseVLESSHeader function
// alone by reusing the reader and buffer.
func BenchmarkParseVLESSHeader_Pure(b *testing.B) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	headerBytes := []byte{
		0x00,                                                  // Version
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		0x00,       // Addons length
		0x01,       // Command (TCP)
		0x01, 0xBB, // Port (443)
		0x01,         // Addr Type (IPv4)
		127, 0, 0, 1, // IP
	}

	r := bytes.NewReader(headerBytes)
	br := bufio.NewReaderSize(r, 512)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Reset(headerBytes)
		br.Reset(r)
		_, _, err := parseVLESSHeader(br, uuid)
		if err != nil {
			b.Fatal(err)
		}
	}
}

var wsSink *wsConn

// BenchmarkWSConnAllocation_Direct measures raw heap allocation of wsConn.
func BenchmarkWSConnAllocation_Direct(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ws := &wsConn{
			Conn:   nil,
			reader: nil,
		}
		wsSink = ws
	}
}

// BenchmarkWSConnAllocation_Pool measures wsConnPool Get/Put allocation (should be 0 allocs/op).
func BenchmarkWSConnAllocation_Pool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ws := wsConnPool.Get().(*wsConn)
		ws.Conn = nil
		ws.reader = nil
		wsConnPool.Put(ws)
	}
}

// BenchmarkSlogLogging measures concurrent structured logging performance with slog.
func BenchmarkSlogLogging(b *testing.B) {
	// Set standard default discard handler to eliminate IO bound benchmarking
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slog.Info("VLESS Connection Closed", "target", "127.0.0.1:443")
		}
	})
}

func TestParseVLESSHeader_Mux(t *testing.T) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	headerBytes := []byte{
		0x00,                                                  // Version
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		0x00, // Addons length
		0x03, // Command (Mux)
	}
	// Append some extra bytes that should remain in the reader after parsing
	extraBytes := []byte{101, 102, 103, 104, 105}
	allBytes := append(headerBytes, extraBytes...)

	r := bytes.NewReader(allBytes)
	br := bufio.NewReaderSize(r, 512)

	targetAddr, cmd, err := parseVLESSHeader(br, uuid)
	if err != nil {
		t.Fatalf("Failed to parse VLESS header: %v", err)
	}

	if cmd != 3 {
		t.Errorf("Expected cmd to be 3, got %d", cmd)
	}

	if targetAddr != "" {
		t.Errorf("Expected targetAddr to be empty, got %q", targetAddr)
	}

	// Verify that the remaining bytes in br are exactly extraBytes
	remaining, err := io.ReadAll(br)
	if err != nil {
		t.Fatalf("Failed to read remaining bytes: %v", err)
	}

	if !bytes.Equal(remaining, extraBytes) {
		t.Errorf("Expected remaining bytes to be %v, got %v", extraBytes, remaining)
	}
}

// BenchmarkParseVLESSHeader_IPv6 measures the performance of parseVLESSHeader on an IPv6 address.
func BenchmarkParseVLESSHeader_IPv6(b *testing.B) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	headerBytes := []byte{
		0x00,                                                  // Version
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		0x00,       // Addons length
		0x01,       // Command (TCP)
		0x01, 0xBB, // Port (443)
		0x03,                                                       // Addr Type (IPv6)
		0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, // IP (2001:db8::1)
	}

	r := bytes.NewReader(headerBytes)
	br := bufio.NewReaderSize(r, 512)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Reset(headerBytes)
		br.Reset(r)
		_, _, err := parseVLESSHeader(br, uuid)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseVLESSHeader_Domain measures the performance of parseVLESSHeader on a domain target.
func BenchmarkParseVLESSHeader_Domain(b *testing.B) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	headerBytes := []byte{
		0x00,                                                  // Version
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		0x00,       // Addons length
		0x01,       // Command (TCP)
		0x01, 0xBB, // Port (443)
		0x02, // Addr Type (Domain)
		11,   // Domain length (11)
		'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
	}

	r := bytes.NewReader(headerBytes)
	br := bufio.NewReaderSize(r, 512)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Reset(headerBytes)
		br.Reset(r)
		_, _, err := parseVLESSHeader(br, uuid)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseVLESSHeader_Mux measures the performance of parseVLESSHeader on a Mux connection.
func BenchmarkParseVLESSHeader_Mux(b *testing.B) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	headerBytes := []byte{
		0x00,                                                  // Version
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		0x00, // Addons length
		0x03, // Command (Mux)
	}

	r := bytes.NewReader(headerBytes)
	br := bufio.NewReaderSize(r, 512)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Reset(headerBytes)
		br.Reset(r)
		_, _, err := parseVLESSHeader(br, uuid)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseUUID measures the performance of uuid parsing helper.
func BenchmarkParseUUID(b *testing.B) {
	uuidStr := "de305d54-75b4-431b-adb2-eb6b9e546013"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := parseUUID(uuidStr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWSConn_ReadHotPath benchmarks the hot-path Read of wsConn where the reader is cached.
func BenchmarkWSConn_ReadHotPath(b *testing.B) {
	payload := make([]byte, 1024)
	ws := &wsConn{}
	r := bytes.NewReader(payload)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Reset(payload)
		ws.reader = r
		var readBuf [512]byte
		_, _ = ws.Read(readBuf[:]) // Read first 512 bytes
		_, _ = ws.Read(readBuf[:]) // Read remaining 512 bytes (no EOF yet)
	}
}

type mockMuxDialer struct {
	mu          sync.Mutex
	dialedNets  []string
	dialedAddrs []string
	conn        net.Conn
	err         error
}

func (d *mockMuxDialer) Dial(network, address string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dialedNets = append(d.dialedNets, network)
	d.dialedAddrs = append(d.dialedAddrs, address)
	return d.conn, d.err
}

func (d *mockMuxDialer) GetDialed() ([]string, []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	nets := make([]string, len(d.dialedNets))
	addrs := make([]string, len(d.dialedAddrs))
	copy(nets, d.dialedNets)
	copy(addrs, d.dialedAddrs)
	return nets, addrs
}

func TestMuxServer_StatusNew(t *testing.T) {
	// 1. Create clientMux and serverMux pipes
	clientMux, serverMux := net.Pipe()
	defer clientMux.Close()
	defer serverMux.Close()

	// 2. Create target pipes
	clientTarget, serverTarget := net.Pipe()
	defer clientTarget.Close()
	defer serverTarget.Close()

	dialer := &mockMuxDialer{
		conn: serverTarget,
	}

	server := NewMuxServer(dialer)

	// Run Serve in a background goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(serverMux, serverMux)
	}()

	// 3. Construct a standard V2Ray Mux frame for StatusNew.
	metaData := &bytes.Buffer{}
	binary.Write(metaData, binary.BigEndian, uint16(42)) // SessionID
	metaData.WriteByte(muxStatusNew)                     // Status
	metaData.WriteByte(muxOptData)                       // Option

	metaData.WriteByte(1)                                // Network: TCP
	binary.Write(metaData, binary.BigEndian, uint16(80)) // Port
	metaData.WriteByte(2)                                // AddressType: Domain
	metaData.WriteByte(7)                                // Domain length
	metaData.WriteString("example")                      // Domain name

	frame := &bytes.Buffer{}
	binary.Write(frame, binary.BigEndian, uint16(metaData.Len()))
	frame.Write(metaData.Bytes())
	binary.Write(frame, binary.BigEndian, uint16(5)) // DataLen
	frame.WriteString("hello")                       // DataContent

	// Write StatusNew frame to clientMux in a background goroutine
	go func() {
		_, _ = clientMux.Write(frame.Bytes())
	}()

	// 4. Read initial data from clientTarget (written by Server to target connection)
	readTargetBuf := make([]byte, 5)
	_, err := io.ReadFull(clientTarget, readTargetBuf)
	if err != nil {
		t.Fatalf("failed to read written data from target connection: %v", err)
	}
	if string(readTargetBuf) != "hello" {
		t.Errorf("expected target to receive 'hello', got '%s'", string(readTargetBuf))
	}

	// 5. Verify Dial parameters
	dialedNets, dialedAddrs := dialer.GetDialed()
	if len(dialedNets) == 0 || dialedNets[0] != "tcp" {
		t.Errorf("expected dialed network tcp, got %v", dialedNets)
	}
	if len(dialedAddrs) == 0 || dialedAddrs[0] != "example:80" {
		t.Errorf("expected dialed address example:80, got %v", dialedAddrs)
	}

	// 6. Write response data from target back to client (which goes through startReadLoop of Server)
	go func() {
		_, _ = clientTarget.Write([]byte("world"))
	}()

	// 7. Read response frame from clientMux
	// Response Frame: MetaLen(2), SessionID(2), StatusKeep(1), OptData(1), DataLen(2), "world"
	var metaLenBuf [2]byte
	_, err = io.ReadFull(clientMux, metaLenBuf[:])
	if err != nil {
		t.Fatalf("failed to read response meta length: %v", err)
	}
	metaLen := binary.BigEndian.Uint16(metaLenBuf[:])
	if metaLen != 4 {
		t.Errorf("expected metaLen 4, got %d", metaLen)
	}

	metaBytes := make([]byte, metaLen)
	_, err = io.ReadFull(clientMux, metaBytes)
	if err != nil {
		t.Fatalf("failed to read response metadata: %v", err)
	}

	sessionID := binary.BigEndian.Uint16(metaBytes[0:2])
	if sessionID != 42 {
		t.Errorf("expected sessionID 42, got %d", sessionID)
	}
	if metaBytes[2] != muxStatusKeep {
		t.Errorf("expected status keep, got %d", metaBytes[2])
	}
	if metaBytes[3] != muxOptData {
		t.Errorf("expected opt data, got %d", metaBytes[3])
	}

	var dataLenBuf [2]byte
	_, err = io.ReadFull(clientMux, dataLenBuf[:])
	if err != nil {
		t.Fatalf("failed to read response data length: %v", err)
	}
	dataLen := binary.BigEndian.Uint16(dataLenBuf[:])
	if dataLen != 5 {
		t.Errorf("expected dataLen 5, got %d", dataLen)
	}

	payloadBytes := make([]byte, dataLen)
	_, err = io.ReadFull(clientMux, payloadBytes)
	if err != nil {
		t.Fatalf("failed to read response data payload: %v", err)
	}
	if string(payloadBytes) != "world" {
		t.Errorf("expected world, got %s", string(payloadBytes))
	}

	// Close clientMux to trigger Serve EOF loop termination
	clientMux.Close()
	err = <-errChan
	if err != io.EOF && err != nil {
		t.Errorf("expected EOF or nil on server exit, got %v", err)
	}
}

func BenchmarkMuxServer_Relay(b *testing.B) {
	// Create clientMux and serverMux pipes
	clientMux, serverMux := net.Pipe()
	defer clientMux.Close()
	defer serverMux.Close()

	// Create target pipes
	clientTarget, serverTarget := net.Pipe()
	defer clientTarget.Close()
	defer serverTarget.Close()

	dialer := &mockMuxDialer{
		conn: serverTarget,
	}

	server := NewMuxServer(dialer)

	// Run Serve in a background goroutine
	go func() {
		_ = server.Serve(serverMux, serverMux)
	}()

	// 1. Establish session (muxStatusNew)
	metaData := &bytes.Buffer{}
	binary.Write(metaData, binary.BigEndian, uint16(1)) // SessionID = 1
	metaData.WriteByte(muxStatusNew)
	metaData.WriteByte(muxOptData)
	metaData.WriteByte(1) // TCP
	binary.Write(metaData, binary.BigEndian, uint16(80))
	metaData.WriteByte(1) // IPv4
	metaData.Write([]byte{127, 0, 0, 1})

	frame := &bytes.Buffer{}
	binary.Write(frame, binary.BigEndian, uint16(metaData.Len()))
	frame.Write(metaData.Bytes())
	binary.Write(frame, binary.BigEndian, uint16(5))
	frame.WriteString("hello")

	_, _ = clientMux.Write(frame.Bytes())

	// Read the "hello" from target
	tmp := make([]byte, 5)
	_, _ = io.ReadFull(clientTarget, tmp)

	// Prepare data to send repeatedly
	payload := make([]byte, 4096) // 4KB chunk

	// Create another buffer for Mux Frame to write to clientMux
	// MetaLen(2) + SessionID(2) + StatusKeep(1) + OptData(1) + DataLen(2) + Payload(4096) = 4104 bytes
	muxFrame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint16(muxFrame[0:2], 4)
	binary.BigEndian.PutUint16(muxFrame[2:4], 1) // SessionID = 1
	muxFrame[4] = muxStatusKeep
	muxFrame[5] = muxOptData
	binary.BigEndian.PutUint16(muxFrame[6:8], uint16(len(payload)))
	copy(muxFrame[8:], payload)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	// Goroutine to drain clientMux responses (prevent blocking)
	go func() {
		buf := make([]byte, 32*1024)
		for {
			_, err := clientMux.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		// Client writes 4KB payload wrapped in Mux frame
		_, err := clientMux.Write(muxFrame)
		if err != nil {
			b.Fatal(err)
		}
		// Target reads the 4KB payload
		_, err = io.ReadFull(clientTarget, payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestParseUUID(t *testing.T) {
	tests := []struct {
		name    string
		uuidStr string
		wantErr bool
	}{
		{"Standard UUID", "de305d54-75b4-431b-adb2-eb6b9e546013", false},
		{"Without Dashes", "de305d5475b4431badb2eb6b9e546013", false},
		{"Too Short", "de305d54-75b4", true},
		{"Too Long", "de305d54-75b4-431b-adb2-eb6b9e546013-123", true},
		{"Invalid Hex", "ge305d54-75b4-431b-adb2-eb6b9e546013", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseUUID(tt.uuidStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUUID() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseVLESSHeader_Failures(t *testing.T) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// 1. Invalid Version (not 0)
	invalidVersion := []byte{
		0x01,                                                  // Version 1
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		0x00, // Addons length
		0x01, // Command (TCP)
	}
	r := bytes.NewReader(invalidVersion)
	br := bufio.NewReader(r)
	_, _, err := parseVLESSHeader(br, uuid)
	if err == nil || !strings.Contains(err.Error(), "unsupported version") {
		t.Errorf("expected unsupported version error, got %v", err)
	}

	// 2. Unauthorized UUID
	badUUID := []byte{
		0x00,                                                  // Version
		1, 1, 1, 1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // Bad UUID
		0x00, // Addons length
		0x01, // Command (TCP)
	}
	r = bytes.NewReader(badUUID)
	br = bufio.NewReader(r)
	_, _, err = parseVLESSHeader(br, uuid)
	if err == nil || !strings.Contains(err.Error(), "unauthorised client UUID") {
		t.Errorf("expected unauthorised UUID error, got %v", err)
	}

	// 3. Unsupported Address Type
	badAddrType := []byte{
		0x00,                                                  // Version
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // UUID
		0x00,       // Addons length
		0x01,       // Command (TCP)
		0x01, 0xBB, // Port (443)
		0x04, // Addr Type (invalid 4)
	}
	r = bytes.NewReader(badAddrType)
	br = bufio.NewReader(r)
	_, _, err = parseVLESSHeader(br, uuid)
	if err == nil || !strings.Contains(err.Error(), "unsupported address type") {
		t.Errorf("expected unsupported address type error, got %v", err)
	}
}

func TestIntegration_VLESS_TCP(t *testing.T) {
	// Initialize global variables used by handleVLESS
	uuidStr := "de305d54-75b4-431b-adb2-eb6b9e546013"
	parsedUUID, _ := parseUUID(uuidStr)
	clientUUID = parsedUUID

	// 1. Create a mock TCP echo server to act as the target "Freedom" outbound destination
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start echo listener: %v", err)
	}
	defer echoListener.Close()

	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn) // Echo back everything
			}()
		}
	}()

	echoAddr := echoListener.Addr().String()
	_, echoPortStr, _ := net.SplitHostPort(echoAddr)
	echoPort, _ := strconv.Atoi(echoPortStr)

	// 2. Start handleVLESS on a httptest Server
	muxHandler := http.NewServeMux()
	muxHandler.HandleFunc("/vless", handleVLESS)
	ts := httptest.NewServer(muxHandler)
	defer ts.Close()

	// 3. Connect a real WebSocket client to handleVLESS
	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/vless"
	wsClient, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer wsClient.Close()

	// 4. Construct VLESS Header with target TCP address pointing to echo server
	headerBytes := []byte{
		0x00, // Version
		clientUUID[0], clientUUID[1], clientUUID[2], clientUUID[3],
		clientUUID[4], clientUUID[5], clientUUID[6], clientUUID[7],
		clientUUID[8], clientUUID[9], clientUUID[10], clientUUID[11],
		clientUUID[12], clientUUID[13], clientUUID[14], clientUUID[15], // UUID
		0x00,                                // Addons length
		0x01,                                // Command: TCP
		byte(echoPort >> 8), byte(echoPort), // Port
		0x01,         // Addr Type: IPv4
		127, 0, 0, 1, // IP: 127.0.0.1
	}

	// Payload data to send right after the VLESS header in the same WS message
	payload := []byte("hello integration test")
	fullMessage := append(headerBytes, payload...)

	// Send VLESS header + initial payload as a single binary message
	err = wsClient.WriteMessage(websocket.BinaryMessage, fullMessage)
	if err != nil {
		t.Fatalf("failed to send VLESS request: %v", err)
	}

	// 5. Read VLESS Response Header (2 bytes: 0x00, 0x00)
	msgType, resp, err := wsClient.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read VLESS response: %v", err)
	}
	if msgType != websocket.BinaryMessage {
		t.Errorf("expected binary message type, got %d", msgType)
	}
	if len(resp) != 2 || resp[0] != 0x00 || resp[1] != 0x00 {
		t.Errorf("expected VLESS response [0x00, 0x00], got %v", resp)
	}

	// 6. Read echoed payload from subsequent WebSocket message
	msgType, echoedMsg, err := wsClient.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read echoed payload: %v", err)
	}
	if string(echoedMsg) != "hello integration test" {
		t.Errorf("expected echoed 'hello integration test', got %q", string(echoedMsg))
	}
}

func TestIntegration_VLESS_Mux(t *testing.T) {
	// Initialize global variables
	uuidStr := "de305d54-75b4-431b-adb2-eb6b9e546013"
	parsedUUID, _ := parseUUID(uuidStr)
	clientUUID = parsedUUID

	// 1. Create a mock TCP echo server
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start echo listener: %v", err)
	}
	defer echoListener.Close()

	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	echoAddr := echoListener.Addr().String()
	_, echoPortStr, _ := net.SplitHostPort(echoAddr)
	echoPort, _ := strconv.Atoi(echoPortStr)

	// 2. Start handleVLESS server
	muxHandler := http.NewServeMux()
	muxHandler.HandleFunc("/vless", handleVLESS)
	ts := httptest.NewServer(muxHandler)
	defer ts.Close()

	// 3. Connect client
	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/vless"
	wsClient, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer wsClient.Close()

	// 4. Send VLESS Header with Command 0x03 (Mux)
	headerBytes := []byte{
		0x00, // Version
		clientUUID[0], clientUUID[1], clientUUID[2], clientUUID[3],
		clientUUID[4], clientUUID[5], clientUUID[6], clientUUID[7],
		clientUUID[8], clientUUID[9], clientUUID[10], clientUUID[11],
		clientUUID[12], clientUUID[13], clientUUID[14], clientUUID[15], // UUID
		0x00, // Addons length
		0x03, // Command: Mux
	}

	err = wsClient.WriteMessage(websocket.BinaryMessage, headerBytes)
	if err != nil {
		t.Fatalf("failed to send VLESS Mux header: %v", err)
	}

	// 5. Read VLESS Response Header
	_, resp, err := wsClient.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read VLESS response: %v", err)
	}
	if len(resp) != 2 || resp[0] != 0x00 || resp[1] != 0x00 {
		t.Fatalf("expected VLESS response [0x00, 0x00], got %v", resp)
	}

	// 6. Now, the connection is upgraded to Mux. We can send Mux frames!
	// Send StatusNew for Session ID 10 pointing to the echo server, carrying initial data "mux data"
	metaData := &bytes.Buffer{}
	binary.Write(metaData, binary.BigEndian, uint16(10)) // SessionID = 10
	metaData.WriteByte(muxStatusNew)
	metaData.WriteByte(muxOptData)
	metaData.WriteByte(1) // TCP
	binary.Write(metaData, binary.BigEndian, uint16(echoPort))
	metaData.WriteByte(1) // IPv4
	metaData.Write([]byte{127, 0, 0, 1})

	frame := &bytes.Buffer{}
	binary.Write(frame, binary.BigEndian, uint16(metaData.Len()))
	frame.Write(metaData.Bytes())
	binary.Write(frame, binary.BigEndian, uint16(8)) // DataLen = 8
	frame.WriteString("mux data")

	err = wsClient.WriteMessage(websocket.BinaryMessage, frame.Bytes())
	if err != nil {
		t.Fatalf("failed to write Mux frame: %v", err)
	}

	// 7. Read Mux response frame from WS
	_, resp, err = wsClient.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read Mux response frame: %v", err)
	}

	// Verify Mux Frame structure: MetaLen(2), SessionID(2), StatusKeep(1), OptData(1), DataLen(2), "mux data"
	if len(resp) < 8 {
		t.Fatalf("Mux response frame too short: %d", len(resp))
	}
	metaLen := binary.BigEndian.Uint16(resp[0:2])
	if metaLen != 4 {
		t.Errorf("expected metaLen 4, got %d", metaLen)
	}
	sessionID := binary.BigEndian.Uint16(resp[2:4])
	if sessionID != 10 {
		t.Errorf("expected sessionID 10, got %d", sessionID)
	}
	if resp[4] != muxStatusKeep {
		t.Errorf("expected status keep, got %d", resp[4])
	}
	if resp[5] != muxOptData {
		t.Errorf("expected opt data, got %d", resp[5])
	}
	dataLen := binary.BigEndian.Uint16(resp[6:8])
	if dataLen != 8 {
		t.Errorf("expected dataLen 8, got %d", dataLen)
	}
	if string(resp[8:16]) != "mux data" {
		t.Errorf("expected 'mux data', got %q", string(resp[8:16]))
	}
}

func TestMuxServer_MetadataBounds(t *testing.T) {
	// Test case 1: Stack path (domain name = "short.com", total metadata < 256)
	metaData1 := &bytes.Buffer{}
	binary.Write(metaData1, binary.BigEndian, uint16(101)) // SessionID
	metaData1.WriteByte(muxStatusNew)
	metaData1.WriteByte(0) // No OptionData
	metaData1.WriteByte(1) // Network: TCP
	binary.Write(metaData1, binary.BigEndian, uint16(80))
	metaData1.WriteByte(2) // AddressType: Domain
	metaData1.WriteByte(9) // Domain length
	metaData1.WriteString("short.com")

	frame1 := &bytes.Buffer{}
	binary.Write(frame1, binary.BigEndian, uint16(metaData1.Len()))
	frame1.Write(metaData1.Bytes())

	// Test case 2: Heap path (long domain name of 220 characters, total metadata > 256)
	longDomain := strings.Repeat("a", 220) + ".com" // 224 chars
	metaData2 := &bytes.Buffer{}
	binary.Write(metaData2, binary.BigEndian, uint16(102)) // SessionID
	metaData2.WriteByte(muxStatusNew)
	metaData2.WriteByte(0) // No OptionData
	metaData2.WriteByte(1) // Network: TCP
	binary.Write(metaData2, binary.BigEndian, uint16(80))
	metaData2.WriteByte(2) // AddressType: Domain
	metaData2.WriteByte(byte(len(longDomain)))
	metaData2.WriteString(longDomain)

	frame2 := &bytes.Buffer{}
	binary.Write(frame2, binary.BigEndian, uint16(metaData2.Len()))
	frame2.Write(metaData2.Bytes())

	// Combine frames
	inputBytes := append(frame1.Bytes(), frame2.Bytes()...)
	r := bytes.NewReader(inputBytes)
	w := &bytes.Buffer{}

	clientTarget, serverTarget := net.Pipe()
	defer clientTarget.Close()
	defer serverTarget.Close()

	dialer := &mockMuxDialer{
		conn: serverTarget,
	}

	server := NewMuxServer(dialer)

	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(r, w)
	}()

	// Wait a moment for reading first frame and second frame
	time.Sleep(30 * time.Millisecond)

	_, dialedAddrs := dialer.GetDialed()
	if len(dialedAddrs) < 2 {
		t.Fatalf("expected at least 2 dialed addresses, got %v", dialedAddrs)
	}

	if dialedAddrs[0] != "short.com:80" {
		t.Errorf("expected short.com:80, got %s", dialedAddrs[0])
	}

	if !strings.HasPrefix(dialedAddrs[1], "aaaa") || !strings.HasSuffix(dialedAddrs[1], "com:80") {
		t.Errorf("expected long domain address, got %s", dialedAddrs[1])
	}

	server.CloseAll()
	_ = <-errChan
}

func TestMuxSession_DrainReclaim(t *testing.T) {
	dialer := &mockMuxDialer{}
	server := NewMuxServer(dialer)

	sess := &MuxSession{
		id:     1,
		ch:     make(chan muxPayload, 10),
		closed: make(chan struct{}),
		server: server,
	}

	// Retrieve buffers from payloadPool
	ptr1 := muxPayloadPool.Get().(*[]byte)
	ptr2 := muxPayloadPool.Get().(*[]byte)

	sess.ch <- muxPayload{ptr: ptr1, data: (*ptr1)[:100]}
	sess.ch <- muxPayload{ptr: ptr2, data: (*ptr2)[:200]}

	// Trigger local close which executes the async-safe drain loop
	sess.closeLocal()

	// Assert that session's closed channel is closed
	select {
	case <-sess.closed:
		// Success
	default:
		t.Error("expected session.closed to be closed")
	}

	// Assert that session channel is drained
	select {
	case <-sess.ch:
		t.Error("expected sess.ch to be empty after drain")
	default:
		// Drained successfully
	}
}

// TestMuxServer_RemoveSession_CompareAndDelete verifies the ABA guard: a stale
// session closing must NOT evict a live new session that reused the same id.
func TestMuxServer_RemoveSession_CompareAndDelete(t *testing.T) {
	server := NewMuxServer(&mockMuxDialer{})

	const id = 5
	oldSess := &MuxSession{id: id, ch: make(chan muxPayload, 1), closed: make(chan struct{}), server: server}
	newSess := &MuxSession{id: id, ch: make(chan muxPayload, 1), closed: make(chan struct{}), server: server}

	// New session is the current occupant of id=5.
	server.mu.Lock()
	server.sessions[id] = newSess
	server.mu.Unlock()

	// The OLD session tears down late and tries to remove id=5. Because the map
	// now points at newSess, the compare-and-delete must leave it untouched.
	server.removeSession(id, oldSess)

	if got := server.getSession(id); got != newSess {
		t.Fatalf("stale removeSession evicted the live session: got %v, want newSess", got)
	}

	// A self-remove by the current occupant must still work.
	server.removeSession(id, newSess)
	if got := server.getSession(id); got != nil {
		t.Fatalf("self removeSession failed to evict: got %v, want nil", got)
	}
}

// TestMuxServer_AddSession_ClosesDisplaced verifies that opening a new session
// on an id already in use closes the displaced session (no goroutine/fd leak)
// while leaving the new session installed.
func TestMuxServer_AddSession_ClosesDisplaced(t *testing.T) {
	server := NewMuxServer(&mockMuxDialer{})
	server.writeCh = make(chan muxWriteFrame, 16) // closeLocal path touches nothing that needs the loop, but be safe

	const id = 9
	displaced := &MuxSession{id: id, ch: make(chan muxPayload, 1), closed: make(chan struct{}), server: server}
	server.addSession(displaced)

	replacement := &MuxSession{id: id, ch: make(chan muxPayload, 1), closed: make(chan struct{}), server: server}
	server.addSession(replacement)

	// The replacement must be the live occupant.
	if got := server.getSession(id); got != replacement {
		t.Fatalf("expected replacement to occupy id=%d, got %v", id, got)
	}

	// The displaced session must have been closed (its closed channel signalled).
	select {
	case <-displaced.closed:
		// Good: displaced session was torn down.
	default:
		t.Fatal("displaced session was not closed on id reuse")
	}

	// And closing the displaced session must NOT have evicted the replacement
	// (closeLocal -> removeSession compare-and-delete).
	if got := server.getSession(id); got != replacement {
		t.Fatalf("closing displaced session wrongly evicted replacement: got %v", got)
	}
}

func TestMuxServer_ConcurrentWrites(t *testing.T) {
	dialer := &mockMuxDialer{}
	server := NewMuxServer(dialer)

	clientMux, serverMux := net.Pipe()
	defer clientMux.Close()
	defer serverMux.Close()

	server.writer = serverMux
	server.writeCh = make(chan muxWriteFrame, 1024)
	server.closed = false

	writeLoopDone := make(chan struct{})
	go func() {
		server.writeLoop()
		close(writeLoopDone)
	}()

	// Simulating multiple concurrent writers
	var wg sync.WaitGroup
	const numGoroutines = 50
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			bufPtr := muxBufPool.Get().(*[]byte)
			buf := *bufPtr
			binary.BigEndian.PutUint16(buf[0:2], uint16(id))
			server.writeFrame(muxWriteFrame{
				bufPtr: bufPtr,
				data:   buf[:2],
			})
		}(i)
	}

	wg.Wait()

	// Drain clientMux in background to unblock writeLoop
	go func() {
		tmp := make([]byte, 100)
		for {
			_, err := clientMux.Read(tmp)
			if err != nil {
				return
			}
		}
	}()

	// Shutdown the server writeLoop cleanly
	server.writeMu.Lock()
	server.closed = true
	close(server.writeCh)
	server.writeMu.Unlock()

	<-writeLoopDone
}

// withShortUDPIdleTimeout temporarily shrinks the package-level udpIdleTimeout
// so timeout-driven tests run fast, restoring the original on cleanup.
func withShortUDPIdleTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	orig := udpIdleTimeout
	udpIdleTimeout = d
	t.Cleanup(func() { udpIdleTimeout = orig })
}

// blockingWriter is an io.Writer that never errors and discards everything.
// Used as the WS-side sink when the downlink should never produce data.
type blockingWriter struct{}

func (blockingWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestRelayUDPDownlink_IdleTimeout verifies that the bare-UDP downlink relay
// tears itself down when the target goes silent, instead of blocking forever
// on dest.Read. Without the SetReadDeadline this goroutine would leak.
func TestRelayUDPDownlink_IdleTimeout(t *testing.T) {
	withShortUDPIdleTimeout(t, 100*time.Millisecond)

	// A real UDP socket pair: SetReadDeadline must behave exactly as in prod.
	targetSrv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer targetSrv.Close()

	dest, err := net.DialUDP("udp", nil, targetSrv.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial udp: %v", err)
	}
	// dest is closed by relayUDPDownlink's defer.

	errChan := make(chan error, 1)
	go relayUDPDownlink(blockingWriter{}, dest, errChan)

	// The target never sends anything, so the read must time out and the
	// relay must return well before the test deadline.
	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("expected a timeout error from idle downlink, got nil")
		}
		nerr, ok := err.(net.Error)
		if !ok || !nerr.Timeout() {
			t.Fatalf("expected a net.Error timeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relayUDPDownlink did not return; idle UDP read leaked")
	}
}

// TestRelayUDPDownlink_StaysAliveWhileActive verifies the deadline is refreshed
// on every successful packet, so a steadily-active target is NOT torn down even
// though each gap is shorter than the idle timeout.
func TestRelayUDPDownlink_StaysAliveWhileActive(t *testing.T) {
	withShortUDPIdleTimeout(t, 150*time.Millisecond)

	targetSrv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer targetSrv.Close()

	dest, err := net.DialUDP("udp", nil, targetSrv.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial udp: %v", err)
	}

	// Prime the server with the client's source addr by sending one packet,
	// so the server knows where to reply.
	if _, err := dest.Write([]byte("hi")); err != nil {
		t.Fatalf("prime write: %v", err)
	}
	buf := make([]byte, 16)
	_ = targetSrv.SetReadDeadline(time.Now().Add(time.Second))
	_, clientAddr, err := targetSrv.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("server read prime: %v", err)
	}

	errChan := make(chan error, 1)
	go relayUDPDownlink(blockingWriter{}, dest, errChan)

	// Send 5 packets spaced 60ms apart (< 150ms idle timeout). Total ~300ms,
	// which exceeds a single idle window — proving the deadline is refreshed.
	stop := make(chan struct{})
	go func() {
		defer close(stop)
		for i := 0; i < 5; i++ {
			_, _ = targetSrv.WriteToUDP([]byte("pkt"), clientAddr)
			time.Sleep(60 * time.Millisecond)
		}
	}()

	select {
	case err := <-errChan:
		t.Fatalf("relay exited early while target was active: %v", err)
	case <-stop:
		// Good: relay survived the whole active period.
	}

	// Now stop sending; the relay should time out shortly after.
	select {
	case <-errChan:
		// Expected eventual idle teardown.
	case <-time.After(time.Second):
		t.Fatal("relay did not time out after target went silent")
	}
}

// TestMuxSession_UDP_IdleTimeout verifies that a Mux UDP sub-stream whose target
// goes silent is torn down by the idle deadline: startReadLoop returns, Close()
// fires, and a StatusEnd frame is emitted to the client.
func TestMuxSession_UDP_IdleTimeout(t *testing.T) {
	withShortUDPIdleTimeout(t, 100*time.Millisecond)

	// Real UDP target so SetReadDeadline behaves as in production.
	targetSrv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer targetSrv.Close()

	dest, err := net.DialUDP("udp", nil, targetSrv.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial udp: %v", err)
	}

	// Drive the server with a real Serve loop over a pipe so the StatusEnd
	// frame surfaces on the client side, then inject a pre-built UDP session.
	clientMux, serverMux := net.Pipe()
	defer clientMux.Close()
	defer serverMux.Close()

	dialer := &mockMuxDialer{conn: dest}
	server := NewMuxServer(dialer)

	errChan := make(chan error, 1)
	go func() { errChan <- server.Serve(serverMux, serverMux) }()
	defer func() {
		clientMux.Close()
		<-errChan
	}()

	// Send a StatusNew UDP frame (network byte = 2) targeting the loopback
	// listener. The address bytes are arbitrary loopback; the mock dialer
	// returns our real UDP `dest` regardless.
	port := targetSrv.LocalAddr().(*net.UDPAddr).Port
	meta := &bytes.Buffer{}
	binary.Write(meta, binary.BigEndian, uint16(7)) // SessionID = 7
	meta.WriteByte(muxStatusNew)
	meta.WriteByte(0)                                  // no OptData
	meta.WriteByte(2)                                  // network = UDP
	binary.Write(meta, binary.BigEndian, uint16(port)) // port
	meta.WriteByte(1)                                  // IPv4
	meta.Write([]byte{127, 0, 0, 1})

	frame := &bytes.Buffer{}
	binary.Write(frame, binary.BigEndian, uint16(meta.Len()))
	frame.Write(meta.Bytes())

	if _, err := clientMux.Write(frame.Bytes()); err != nil {
		t.Fatalf("write StatusNew frame: %v", err)
	}

	// The target never replies, so startReadLoop's idle deadline must fire,
	// Close() runs, and a StatusEnd frame (6 bytes) arrives on clientMux.
	done := make(chan []byte, 1)
	go func() {
		var hdr [2]byte
		if _, err := io.ReadFull(clientMux, hdr[:]); err != nil {
			return
		}
		metaLen := binary.BigEndian.Uint16(hdr[:])
		body := make([]byte, metaLen)
		if _, err := io.ReadFull(clientMux, body); err != nil {
			return
		}
		done <- body
	}()

	select {
	case body := <-done:
		if len(body) < 3 {
			t.Fatalf("StatusEnd frame too short: %v", body)
		}
		if got := binary.BigEndian.Uint16(body[0:2]); got != 7 {
			t.Errorf("expected sessionID 7 in End frame, got %d", got)
		}
		if body[2] != muxStatusEnd {
			t.Errorf("expected StatusEnd (0x%02x), got 0x%02x", muxStatusEnd, body[2])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no StatusEnd received; idle UDP sub-stream read leaked")
	}
}

// TestMuxSession_TCP_NoIdleTimeout is the regression guard for the central
// design decision: TCP sub-streams must NEVER carry an idle read deadline, so a
// legitimately idle TCP connection (SSH, long-poll) is not torn down. We assert
// that an idle TCP sub-stream emits no StatusEnd frame within several idle
// windows.
func TestMuxSession_TCP_NoIdleTimeout(t *testing.T) {
	withShortUDPIdleTimeout(t, 80*time.Millisecond)

	// A real TCP target that accepts the connection and then stays silent.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	defer ln.Close()

	acceptedConns := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		acceptedConns <- c // hold it open, never write
	}()

	dest, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial tcp: %v", err)
	}

	clientMux, serverMux := net.Pipe()
	defer clientMux.Close()
	defer serverMux.Close()

	dialer := &mockMuxDialer{conn: dest}
	server := NewMuxServer(dialer)

	errChan := make(chan error, 1)
	go func() { errChan <- server.Serve(serverMux, serverMux) }()
	defer func() {
		clientMux.Close()
		<-errChan
		if c := <-acceptedConns; c != nil {
			c.Close()
		}
	}()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	meta := &bytes.Buffer{}
	binary.Write(meta, binary.BigEndian, uint16(9)) // SessionID = 9
	meta.WriteByte(muxStatusNew)
	meta.WriteByte(0)                                  // no OptData
	meta.WriteByte(1)                                  // network = TCP
	binary.Write(meta, binary.BigEndian, uint16(port)) // port
	meta.WriteByte(1)                                  // IPv4
	meta.Write([]byte{127, 0, 0, 1})

	frame := &bytes.Buffer{}
	binary.Write(frame, binary.BigEndian, uint16(meta.Len()))
	frame.Write(meta.Bytes())

	if _, err := clientMux.Write(frame.Bytes()); err != nil {
		t.Fatalf("write StatusNew frame: %v", err)
	}

	// Watch for any frame from the server. If the TCP sub-stream wrongly timed
	// out, a StatusEnd frame would appear here.
	gotFrame := make(chan struct{}, 1)
	go func() {
		var hdr [2]byte
		if _, err := io.ReadFull(clientMux, hdr[:]); err != nil {
			return
		}
		gotFrame <- struct{}{}
	}()

	// Wait well past several idle windows (80ms * ~6). A correct TCP path
	// produces nothing in this period.
	select {
	case <-gotFrame:
		t.Fatal("TCP sub-stream was torn down by idle timeout; it must stay open")
	case <-time.After(500 * time.Millisecond):
		// Good: TCP sub-stream remained open despite being idle.
	}
}

// ============================================================================
// Optimization-verification benchmarks
//
// These benchmarks isolate the per-connection-setup cost that the three memory
// optimizations target. Each pairs an "Old" variant (the pre-optimization
// behavior, reconstructed inline) against a "New" variant (the current pooled
// path) so the allocation delta is directly visible in `-benchmem` output.
// Run: go test -run=^$ -bench='ConnSetup|MuxPool|SessionChan' -benchmem
// ============================================================================

// connSetupOld reproduces the pre-optimization handleVLESS prologue: a freshly
// heap-allocated wsConn wrapper plus a freshly allocated 512-byte bufio.Reader
// on every connection.
//
//go:noinline
func connSetupOld() *bufio.Reader {
	ws := &wsConn{}
	br := bufio.NewReaderSize(ws, 512)
	return br
}

// connSetupNew reproduces the current pooled path: the wsConn wrapper comes from
// wsConnPool and carries a reusable bufio.Reader that is Reset rather than
// reallocated.
//
//go:noinline
func connSetupNew() *bufio.Reader {
	ws := wsConnPool.Get().(*wsConn)
	ws.Conn = nil
	ws.reader = nil
	var br *bufio.Reader
	if ws.br != nil {
		ws.br.Reset(ws)
		br = ws.br
	} else {
		br = bufio.NewReaderSize(ws, 512)
		ws.br = br
	}
	// Mirror handleVLESS teardown.
	ws.Conn = nil
	ws.reader = nil
	ws.br.Reset(nil)
	wsConnPool.Put(ws)
	return br
}

var brSink *bufio.Reader

// BenchmarkConnSetup_Old measures per-connection wrapper+reader allocation
// before bufio.Reader/wsConn pooling. Expect ~2 allocs/op (wsConn + bufio buf).
func BenchmarkConnSetup_Old(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		brSink = connSetupOld()
	}
}

// BenchmarkConnSetup_New measures the pooled path. Expect ~0 allocs/op in steady
// state since both the wsConn and its bufio.Reader are recycled.
func BenchmarkConnSetup_New(b *testing.B) {
	// Warm the pool so the first iteration's lazy bufio.Reader creation does not
	// skew the steady-state numbers.
	brSink = connSetupNew()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brSink = connSetupNew()
	}
}

// BenchmarkMuxPool_PerServer reproduces the old per-MuxServer pool lifecycle:
// each connection built its own pair of sync.Pools, so the first Get always
// allocated a fresh 16 KB slab and the cache died with the server. Modeled here
// by creating a fresh pool every iteration and taking one slab from it.
func BenchmarkMuxPool_PerServer(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := sync.Pool{New: func() interface{} {
			s := make([]byte, muxBufSize)
			return &s
		}}
		ptr := p.Get().(*[]byte)
		bufSink = ptr
	}
}

var bufSink *[]byte

// BenchmarkMuxPool_Global measures the current global pool: a steady-state
// Get/Put pair against the package-level muxBufPool. Expect ~0 allocs/op once
// the pool is warm, since slabs survive across connections.
func BenchmarkMuxPool_Global(b *testing.B) {
	// Warm the global pool.
	warm := muxBufPool.Get().(*[]byte)
	muxBufPool.Put(warm)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ptr := muxBufPool.Get().(*[]byte)
		bufSink = ptr
		muxBufPool.Put(ptr)
	}
}

var chanSink chan muxPayload

// BenchmarkSessionChan_Cap128 measures per-sub-stream channel allocation at the
// old buffer size of 128 (muxPayload is 32 B → ~4 KB backing array).
func BenchmarkSessionChan_Cap128(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		chanSink = make(chan muxPayload, 128)
	}
}

// BenchmarkSessionChan_Cap32 measures the current size. The backing array is a
// quarter the size, which `-benchmem` reports as proportionally fewer bytes/op.
func BenchmarkSessionChan_Cap32(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		chanSink = make(chan muxPayload, muxSessionChanCap)
	}
}

// failingTransport is an io.ReadWriteCloser whose Write always fails (simulating
// a dead downlink / vanished client). Reads are served from an embedded pipe so
// the Serve loop blocks on a real read until Close tears the pipe down — exactly
// like a live-but-idle WebSocket. Close is recorded so the test can assert that
// writeLoop unblocked the read loop on write failure.
type failingTransport struct {
	r         *io.PipeReader
	closeOnce sync.Once
	closed    chan struct{} // signalled when Close runs
}

func (f *failingTransport) Write(p []byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func (f *failingTransport) Read(p []byte) (int, error) {
	return f.r.Read(p)
}

func (f *failingTransport) Close() error {
	f.closeOnce.Do(func() {
		_ = f.r.Close() // unblocks any in-flight Read with io.ErrClosedPipe
		close(f.closed)
	})
	return nil
}

// TestMuxServer_WriteErrorClosesTransport verifies that when a downlink frame
// write fails, writeLoop closes the underlying transport, which unblocks the
// Serve read loop and tears the whole session down promptly — instead of the
// old behavior where the write error was swallowed and Serve kept blocking on
// the read until the client happened to close.
func TestMuxServer_WriteErrorClosesTransport(t *testing.T) {
	// Target connection that emits data, so the sub-stream's readLoop produces a
	// downlink frame and drives writeFrame from inside Serve's own goroutines
	// (never from the test goroutine — that would race on Serve's field init).
	clientTarget, serverTarget := net.Pipe()
	defer clientTarget.Close()
	defer serverTarget.Close()

	dialer := &mockMuxDialer{conn: serverTarget}
	server := NewMuxServer(dialer)

	pr, pw := io.Pipe()
	transport := &failingTransport{r: pr, closed: make(chan struct{})}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(transport, transport)
	}()

	// 1. Open a TCP sub-stream (StatusNew, no payload).
	meta := &bytes.Buffer{}
	binary.Write(meta, binary.BigEndian, uint16(7)) // SessionID
	meta.WriteByte(muxStatusNew)
	meta.WriteByte(0)                                // no OptData
	meta.WriteByte(1)                                // TCP
	binary.Write(meta, binary.BigEndian, uint16(80)) // port
	meta.WriteByte(1)                                // IPv4
	meta.Write([]byte{127, 0, 0, 1})

	frame := &bytes.Buffer{}
	binary.Write(frame, binary.BigEndian, uint16(meta.Len()))
	frame.Write(meta.Bytes())
	if _, err := pw.Write(frame.Bytes()); err != nil {
		t.Fatalf("write StatusNew frame: %v", err)
	}

	// 2. Make the target emit data → readLoop frames it → writeFrame → the
	//    failing downlink Write fires, which must close the transport.
	go func() { _, _ = clientTarget.Write([]byte("downlink data")) }()

	select {
	case <-transport.closed:
		// Good: writeLoop closed the transport on write failure.
	case <-time.After(2 * time.Second):
		t.Fatal("write error did not close the transport")
	}

	// 3. Serve must return promptly now that its read is unblocked.
	select {
	case <-serveDone:
		// Good: session torn down.
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after transport close")
	}
}
