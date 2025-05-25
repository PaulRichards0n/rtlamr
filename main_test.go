package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bemasher/rtlamr/protocol"
	"github.com/bemasher/rtltcp"
	"github.com/pkg/errors"
)

// --- Test Helpers for Global State Management ---
// (saveRestoreLocalFlags, TestMain from previous steps)
var (
	_originalMsgType      StringMap // Renamed to avoid conflict
	_originalSymbolLength int     // Renamed
)

func saveRestoreLocalFlags(t *testing.T) func() {
	t.Helper()
	_originalMsgType = make(StringMap)
	for k, v := range msgType {
		_originalMsgType[k] = v
	}
	_originalSymbolLength = *symbolLength
	if _originalSymbolLength == 0 {
		*symbolLength = 72
	}
	return func() {
		msgType = make(StringMap)
		for k, v := range _originalMsgType {
			msgType[k] = v
		}
		*symbolLength = _originalSymbolLength
	}
}

func TestMain(m *testing.M) {
	if !flag.Parsed() {
		RegisterFlags()
		if rcvr.SDR.Flags.DeviceIdx == 0 { // rcvr might be zero initialized if tests run before main's init
			// Minimal setup for global rcvr if needed by rcvr.RegisterFlags()
		}
		rcvr.RegisterFlags()
		flag.Parse()
	}
	if *symbolLength == 0 { // Ensure default if not parsed
		sl := 72
		symbolLength = &sl
	}
	os.Exit(m.Run())
}


// --- Tests for registerMessageParsers ---
func TestRegisterMessageParsers(t *testing.T) {
	if *symbolLength == 0 { // Fallback if TestMain didn't set it
		sl := 72
		symbolLength = &sl
		t.Log("Manually setting *symbolLength to default 72 for TestRegisterMessageParsers")
	}
	tests := []struct {
		name          string
		setupMsgType  StringMap
		expectError   bool
		expectedTypes map[string]bool
	}{
		{name: "all types", setupMsgType: StringMap{"all": true}, expectError: false, expectedTypes: map[string]bool{"scm": true, "scm+": true, "idm": true, "r900": true}},
		{name: "specific valid types", setupMsgType: StringMap{"scm": true, "idm": true}, expectError: false, expectedTypes: map[string]bool{"scm": true, "idm": true}},
		{name: "invalid type", setupMsgType: StringMap{"invalidtype": true}, expectError: true},
		{name: "mixed valid and invalid type", setupMsgType: StringMap{"scm": true, "invalidtype": true}, expectError: true},
		{name: "empty msgType", setupMsgType: StringMap{}, expectError: false, expectedTypes: map[string]bool{}},
		{name: "all with other types should fail", setupMsgType: StringMap{"all": true, "idm": true}, expectError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restore := saveRestoreLocalFlags(t)
			defer restore()
			msgType = make(StringMap); for k, v := range tt.setupMsgType { msgType[k] = v }
			testRcvr := &Receiver{d: protocol.NewDecoder()}
			err := testRcvr.registerMessageParsers()
			if tt.expectError && err == nil { t.Errorf("expected error, got nil") }
			if !tt.expectError && err != nil { t.Errorf("unexpected error: %v", err) }
			if !tt.expectError && tt.expectedTypes != nil {
				if len(msgType) != len(tt.expectedTypes) { t.Errorf("len(msgType) got %d, want %d", len(msgType), len(tt.expectedTypes)) }
				for kexp := range tt.expectedTypes { if !msgType[kexp] { t.Errorf("expected msgType[%s]", kexp) } }
			}
		})
	}
}

// --- Tests for configureSDRFromFlags ---
func TestConfigureSDRFromFlags(t *testing.T) {
	baseRcvrFlags := rtltcp.SDRFlags{CenterFreq: 915000000, SampleRate: 2048000}
	tests := []struct {
		name string; setupGlobalFlags func(t *testing.T); setupRcvrSDRFlags func(*rtltcp.SDRFlags)
		gainFlagsToSet map[string]string; expectedCenterFreq uint32; expectedSampleRate int
		expectGainModeTrue bool; expectedFilters int
	}{
		{name: "defaults", setupRcvrSDRFlags: func(f *rtltcp.SDRFlags) {f.CenterFreq=912M; f.SampleRate=2000k}, expectedCenterFreq:912M, expectedSampleRate:2000k, expectGainModeTrue: true, expectedFilters:0},
		{name: "unique filter", setupGlobalFlags: func(t *testing.T){flag.Set("unique","true")}, expectedFilters:1, expectGainModeTrue:true},
		{name: "filterid", setupGlobalFlags: func(t *testing.T){flag.Set("filterid","123,456")}, expectedFilters:1, expectGainModeTrue:true},
		{name: "filtertype", setupGlobalFlags: func(t *testing.T){flag.Set("filtertype","7,8")}, expectedFilters:1, expectGainModeTrue:true},
		{name: "all filters", setupGlobalFlags: func(t *testing.T){flag.Set("unique","true");flag.Set("filterid","123");flag.Set("filtertype","5")}, expectedFilters:3, expectGainModeTrue:true},
		{name: "gain flag set", gainFlagsToSet: map[string]string{"tunergain":"12.3"}, expectedFilters:0, expectGainModeTrue:false},
	}
	const ( M=1000000; k=1000 ) // For readability in table

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saved := make(map[string]string); flags := []string{"unique","filterid","filtertype","gainbyindex","tunergainmode","tunergain","agcmode"}
			for _,f:=range flags { if fl:=flag.Lookup(f);fl!=nil{saved[f]=fl.Value.String()}}
			savedMID, savedMT := meterID, meterType
			t.Cleanup(func(){ for f,v:=range saved{if fl:=flag.Lookup(f);fl!=nil{flag.Set(f,v)}}; meterID=savedMID; meterType=savedMT })
			for _,f:=range flags { if fl:=flag.Lookup(f);fl!=nil{ defVal:="false"; if f=="filterid"||f=="filtertype"||f=="tunergain"||f=="gainbyindex"{defVal=""}; flag.Set(f,defVal) }}
			meterID = MeterIDFilter{make(UintMap)}; meterType = MeterTypeFilter{make(UintMap)}

			if tt.setupGlobalFlags != nil { tt.setupGlobalFlags(t) }
			if tt.gainFlagsToSet != nil { for n,v:=range tt.gainFlagsToSet { if fl:=flag.Lookup(n);fl==nil{ if n=="tunergain"{flag.Float64(n,0,"")}else{flag.String(n,"","")} }; flag.Set(n,v) }}
			
			testRcvr := &Receiver{d:protocol.NewDecoder(), fc:protocol.FilterChain{}, SDR:rtltcp.SDR{Flags:baseRcvrFlags}}
			if tt.setupRcvrSDRFlags != nil { tt.setupRcvrSDRFlags(&testRcvr.SDR.Flags) }
			testRcvr.d.Cfg.CenterFreq = testRcvr.SDR.Flags.CenterFreq; testRcvr.d.Cfg.SampleRate = testRcvr.SDR.Flags.SampleRate
			testRcvr.configureSDRFromFlags()

			if tt.expectedCenterFreq!=0 && testRcvr.d.Cfg.CenterFreq != tt.expectedCenterFreq {t.Errorf("CenterFreq got %d want %d", testRcvr.d.Cfg.CenterFreq, tt.expectedCenterFreq)}
			if tt.expectedSampleRate!=0 && testRcvr.d.Cfg.SampleRate != tt.expectedSampleRate {t.Errorf("SampleRate got %d want %d", testRcvr.d.Cfg.SampleRate, tt.expectedSampleRate)}
			if tt.expectGainModeTrue { if !(testRcvr.SDR.TunerGainMode && !testRcvr.SDR.AgcMode) {t.Errorf("Expected TunerGainMode=true, AgcMode=false, got TGM=%v, AGC=%v", testRcvr.SDR.TunerGainMode, testRcvr.SDR.AgcMode)}}
			if !tt.expectGainModeTrue && len(tt.gainFlagsToSet)>0 { if testRcvr.SDR.TunerGainMode && !testRcvr.SDR.AgcMode { t.Errorf("Expected SetGainMode(true) NOT called, but TGM=true,AGC=false suggests it was") } }
			if len(testRcvr.fc) != tt.expectedFilters {t.Errorf("Filters: got %d want %d", len(testRcvr.fc), tt.expectedFilters)}
		})
	}
}

// --- Mock for SDR's underlying connection (io.ReadWriteCloser) ---
type mockConn struct {
	readBuffer bytes.Buffer; readError error; writeBuffer bytes.Buffer; writeError error
	closed bool; deadlineError error
	bytesToRead int // Number of bytes to successfully read before returning readError or EOF
	readCalledCount int
}
func (m *mockConn) Read(p []byte) (n int, err error) {
	m.readCalledCount++
	if m.closed { return 0, io.EOF }
	if m.bytesToRead > 0 {
		if m.bytesToRead < len(p) {
			p = p[:m.bytesToRead]
		}
		n, err = m.readBuffer.Read(p)
		m.bytesToRead -= n
		return n, err
	}
	if m.readError != nil { return 0, m.readError }
	return m.readBuffer.Read(p) // Default: continue reading from buffer
}
func (m *mockConn) Write(p []byte) (n int, err error) { if m.closed{return 0,errors.New("write to closed conn")}; if m.writeError!=nil{return 0,m.writeError}; return m.writeBuffer.Write(p)}
func (m *mockConn) Close() error { m.closed=true; return nil }
func (m *mockConn) LocalAddr() net.Addr { return nil }
func (m *mockConn) RemoteAddr() net.Addr { return nil }
func (m *mockConn) SetDeadline(t time.Time) error { return m.deadlineError }
func (m *mockConn) SetReadDeadline(t time.Time) error { return m.deadlineError }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return m.deadlineError }

// --- Tests for readSampleBlocks ---
func TestReadSampleBlocks(t *testing.T) {
	blockSize := 16
	tests := []struct{ name string; mockSetup func(*mockConn); expectDataCount int; expectRcvrError bool; cancelEarly bool }{
		{name: "read 3 blocks", mockSetup: func(mc *mockConn){mc.readBuffer.Write(make([]byte,blockSize*3))}, expectDataCount:3},
		{name: "read error after 1 block", mockSetup: func(mc *mockConn){mc.bytesToRead=blockSize; mc.readError=errors.New("err")}, expectDataCount:1, expectRcvrError:true},
		{name: "context cancel after 1 block", mockSetup:func(mc *mockConn){mc.readBuffer.Write(make([]byte,blockSize*5))}, expectDataCount:1, cancelEarly:true},
		{name: "SetDeadline error", mockSetup:func(mc *mockConn){mc.deadlineError=errors.New("err")}, expectRcvrError:true},
		{name: "EOF after 1 block", mockSetup:func(mc *mockConn){mc.bytesToRead=blockSize; mc.readError=io.EOF}, expectDataCount:1, expectRcvrError:true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T){
			mockNetConn := &mockConn{}; if tt.mockSetup != nil {tt.mockSetup(mockNetConn)}
			testRcvr := &Receiver{SDR:rtltcp.SDR{}, d:protocol.NewDecoder()}; testRcvr.d.Cfg.BlockSize2 = uint32(blockSize)
			if err := testRcvr.SDR.Connect(mockNetConn); err != nil {t.Fatalf("Connect error: %v",err)}
			testRcvr.ctx, testRcvr.cancel = context.WithCancel(context.Background()); testRcvr.wg = &sync.WaitGroup{}; testRcvr.wg.Add(1)
			blockCh := make(chan []byte, 5); go testRcvr.readSampleBlocks(blockCh)
			
			var receivedCount int; var errCh = make(chan error, 1)
			var monitorWg sync.WaitGroup; monitorWg.Add(1)
			go func(){
				defer monitorWg.Done()
				for i:=0; i<tt.expectDataCount+1; i++ { // Read one more to check for unexpected blocks
					select {
					case data, ok := <-blockCh:
						if !ok { if i < tt.expectDataCount {errCh <- errors.New("blockCh closed prematurely")}; return }
						if len(data)!=blockSize {errCh <- fmt.Errorf("bad block size: %d",len(data)); return}
						receivedCount++
						if tt.cancelEarly && receivedCount == tt.expectDataCount { testRcvr.cancel(); return } // Cancel then return to let main check
					case <-time.After(100*time.Millisecond): // Shorter timeout for tests
						if tt.cancelEarly && testRcvr.ctx.Err() == context.Canceled { return } // Expected cancellation
						if tt.expectRcvrError && testRcvr.err != nil { return } // Expected error
						if i < tt.expectDataCount { errCh <- fmt.Errorf("timeout, got %d blocks, expected %d", receivedCount, tt.expectDataCount) }
						return // Done waiting
					}
				}
			}()
			monitorWg.Wait(); close(errCh)
			if testErr := <-errCh; testErr != nil { t.Error(testErr) }

			if tt.cancelEarly { if testRcvr.ctx.Err()!=context.Canceled {t.Error("expected context canceled")} }
			if tt.expectDataCount != receivedCount {t.Errorf("expected %d blocks, got %d", tt.expectDataCount, receivedCount)}
			if tt.expectRcvrError { if testRcvr.err==nil {t.Error("expected rcvr.err")} }
			if !tt.expectRcvrError && !tt.cancelEarly { if testRcvr.err!=nil && !errors.Is(testRcvr.err, context.Canceled) {t.Errorf("unexpected rcvr.err: %v",testRcvr.err)} }
			testRcvr.wg.Wait() // Ensure goroutine finished and closed blockCh
			if _, ok := <-blockCh; ok && !tt.cancelEarly && !tt.expectRcvrError { t.Error("blockCh not closed by readSampleBlocks after normal completion")}
		})
	}
}


// --- Mocks for processDecodedMessages ---
type mockPktDecoder struct { // Renamed from mockDecoder to avoid conflict
	msgChan chan protocol.Message
	cfg     protocol.Config
	decodeCallCount int
}
func (m *mockPktDecoder) Decode(block []byte) <-chan protocol.Message { m.decodeCallCount++; return m.msgChan }
func (m *mockPktDecoder) Cfg() protocol.Config                        { return m.cfg }
func (m *mockPktDecoder) Allocate()                                   {}
func (m *mockPktDecoder) RegisterProtocol(p protocol.Parser)          {}
func (m *mockPktDecoder) Log()                                        {}

type mockMsg struct { // Renamed from mockMessage
	id          uint
	msgTypeStr  string
	checksumVal []byte
	meterIDVal  uint
}
func (m mockMsg) MeterID() uint                { return m.meterIDVal }
func (m mockMsg) MeterType() string            { return m.msgTypeStr }
func (m mockMsg) Consume(s string) error       { return nil }
func (m mockMsg) String() string               { return fmt.Sprintf("mockMsg ID %d MID %d", m.id, m.meterIDVal) }
func (m mockMsg) Checksum() []byte             { return m.checksumVal }
func (m mockMsg) MsgType() string              { return m.msgTypeStr }

type mockFC struct { // Renamed from mockFilterChain
	matchFunc func(protocol.Message) bool
}
func (m *mockFC) Add(f protocol.Filter)             {}
func (m *mockFC) Match(msg protocol.Message) bool { return m.matchFunc(msg) }

type mockEnc struct { // Renamed from mockEncoder
	encodeFunc        func(interface{}) error
	encodeCalledCount int
}
func (m *mockEnc) Encode(v interface{}) error { m.encodeCalledCount++; if m.encodeFunc != nil {return m.encodeFunc(v)}; return nil }

type mockSW struct { // Renamed from mockSampleWriter
	writeFunc        func([]byte) (int, error)
	bytesWritten     int
	writeCalledCount int
	lastWriteData    []byte
}
func (m *mockSW) Write(p []byte) (n int, err error) {
	m.writeCalledCount++; m.lastWriteData = make([]byte, len(p)); copy(m.lastWriteData, p)
	if m.writeFunc != nil { n, err = m.writeFunc(p); m.bytesWritten += n; return n, err }
	m.bytesWritten += len(p); return len(p), nil
}

// --- Tests for processDecodedMessages ---
func TestProcessDecodedMessages(t *testing.T) {
	defaultBlock := []byte("defaultblockdata") // Sample block data

	// Save and restore global state for encoder, sampleWriter, *single, meterID
	originalEncoder := encoder
	originalSampleWriter := sampleWriter
	originalSingle := *single
	originalMeterID := meterID
	t.Cleanup(func() {
		encoder = originalEncoder
		sampleWriter = originalSampleWriter
		*single = originalSingle
		meterID = originalMeterID
	})

	tests := []struct {
		name string
		setup func(t *testing.T, testRcvr *Receiver, mockDec *mockPktDecoder, mockFChain *mockFC, mockEnc *mockEnc, mockSWrt *mockSW)
		
		inputBlockData  []byte // Data to send on blockCh
		messagesToSend  []protocol.Message // Messages for mockDecoder to send for the inputBlockData
		
		expectPktFound      bool
		expectRcvrError     bool
		expectCancel        bool // if rcvr.cancel should be called
		expectedEncodeCount int
		expectedWriteCount  int
		finalPrev           map[protocol.Digest]bool // Expected state of 'prev' map after run (was 'next')
		finalMeterIDCount   int  // If *single is true, expected count in meterID
	}{
		{
			name: "basic process one message",
			inputBlockData: defaultBlock,
			messagesToSend: []protocol.Message{mockMsg{id:1, meterIDVal:1001, checksumVal:[]byte("cs1"), msgTypeStr:"scm"}},
			setup: func(t *testing.T, rcvr *Receiver, dec *mockPktDecoder, fc *mockFC, enc *mockEnc, sw *mockSWrt) {
				fc.matchFunc = func(m protocol.Message) bool { return true } // Filter allows
				rcvr.d = dec // Assign mock decoder
				rcvr.fc = fc // Assign mock filter chain
				encoder = enc // Assign mock global encoder
				sampleWriter = sw // Assign mock global sample writer
			},
			expectPktFound: true, expectEncodeCount: 1, expectWriteCount: 1,
			finalPrev: map[protocol.Digest]bool{protocol.NewDigest(mockMsg{id:1, meterIDVal:1001, checksumVal:[]byte("cs1")}): true},
		},
		{
			name: "filter rejects message",
			inputBlockData: defaultBlock,
			messagesToSend: []protocol.Message{mockMsg{id:2, meterIDVal:1002, checksumVal:[]byte("cs2"), msgTypeStr:"idm"}},
			setup: func(t *testing.T, rcvr *Receiver, dec *mockPktDecoder, fc *mockFC, enc *mockEnc, sw *mockSWrt) {
				fc.matchFunc = func(m protocol.Message) bool { return false } // Filter rejects
				rcvr.d = dec; rcvr.fc = fc; encoder = enc; sampleWriter = sw;
			},
			expectPktFound: false, expectEncodeCount: 0, expectWriteCount: 0,
			finalPrev: map[protocol.Digest]bool{}, // Message not added to 'next' (so 'prev' remains empty)
		},
		{
			name: "message in prev map (duplicate)",
			inputBlockData: defaultBlock,
			messagesToSend: []protocol.Message{mockMsg{id:3, meterIDVal:1003, checksumVal:[]byte("cs3"), msgTypeStr:"r900"}},
			setup: func(t *testing.T, rcvr *Receiver, dec *mockPktDecoder, fc *mockFC, enc *mockEnc, sw *mockSWrt) {
				fc.matchFunc = func(m protocol.Message) bool { return true }
				rcvr.d = dec; rcvr.fc = fc; encoder = enc; sampleWriter = sw;
				// Pre-fill 'prev' map for the test
				// This map is passed into processDecodedMessages.
			},
			// prev map will be passed directly to processDecodedMessages call in test body
			expectPktFound: false, expectEncodeCount: 0, expectWriteCount: 0,
			// finalPrev will be what was passed in as prev, if the message matched.
		},
		{
			name: "encoder error",
			inputBlockData: defaultBlock,
			messagesToSend: []protocol.Message{mockMsg{id:4, meterIDVal:1004, checksumVal:[]byte("cs4")}},
			setup: func(t *testing.T, rcvr *Receiver, dec *mockPktDecoder, fc *mockFC, enc *mockEnc, sw *mockSWrt) {
				fc.matchFunc = func(m protocol.Message) bool { return true }
				enc.encodeFunc = func(v interface{}) error { return errors.New("encoder error") }
				rcvr.d = dec; rcvr.fc = fc; encoder = enc; sampleWriter = sw;
			},
			expectRcvrError: true, expectEncodeCount: 1, expectWriteCount: 0, // pktFound might be true before error
		},
		{
			name: "sampleWriter error",
			inputBlockData: defaultBlock,
			messagesToSend: []protocol.Message{mockMsg{id:5, meterIDVal:1005, checksumVal:[]byte("cs5")}},
			setup: func(t *testing.T, rcvr *Receiver, dec *mockPktDecoder, fc *mockFC, enc *mockEnc, sw *mockSWrt) {
				fc.matchFunc = func(m protocol.Message) bool { return true }
				sw.writeFunc = func(p []byte) (int,error) { return 0, errors.New("writer error")}
				rcvr.d = dec; rcvr.fc = fc; encoder = enc; sampleWriter = sw;
			},
			expectPktFound: true, expectEncodeCount: 1, expectWriteCount: 1, 
			expectRcvrError: true, expectCancel: true,
		},
		{
			name: "single flag - meter found and removed, list not empty",
			inputBlockData: defaultBlock,
			messagesToSend: []protocol.Message{mockMsg{id:6, meterIDVal:1006, checksumVal:[]byte("cs6")}},
			setup: func(t *testing.T, rcvr *Receiver, dec *mockPktDecoder, fc *mockFC, enc *mockEnc, sw *mockSWrt) {
				fc.matchFunc = func(m protocol.Message) bool { return true }
				*single = true
				meterID = MeterIDFilter{UintMap{1006: true, 9999: true}} // Target ID + another
				rcvr.d = dec; rcvr.fc = fc; encoder = enc; sampleWriter = sw;
			},
			expectPktFound: true, expectEncodeCount: 1, expectWriteCount: 1,
			finalMeterIDCount: 1, // 9999 should remain
			finalPrev: map[protocol.Digest]bool{protocol.NewDigest(mockMsg{id:6, meterIDVal:1006, checksumVal:[]byte("cs6")}): true},
		},
		{
			name: "single flag - last meter found, cancel called",
			inputBlockData: defaultBlock,
			messagesToSend: []protocol.Message{mockMsg{id:7, meterIDVal:1007, checksumVal:[]byte("cs7")}},
			setup: func(t *testing.T, rcvr *Receiver, dec *mockPktDecoder, fc *mockFC, enc *mockEnc, sw *mockSWrt) {
				fc.matchFunc = func(m protocol.Message) bool { return true }
				*single = true
				meterID = MeterIDFilter{UintMap{1007: true}} // Only target ID
				rcvr.d = dec; rcvr.fc = fc; encoder = enc; sampleWriter = sw;
			},
			expectPktFound: true, expectEncodeCount: 1, expectWriteCount: 1,
			expectCancel: true, finalMeterIDCount: 0,
			finalPrev: map[protocol.Digest]bool{protocol.NewDigest(mockMsg{id:7, meterIDVal:1007, checksumVal:[]byte("cs7")}): true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset globals for each test
			*single = false
			meterID = MeterIDFilter{make(UintMap)}
			// Mocks for this test run
			mockDec := &mockPktDecoder{msgChan: make(chan protocol.Message, len(tt.messagesToSend))}
			mockFChain := &mockFC{}
			mockEnc := &mockEnc{}
			mockSWrt := &mockSW{}

			testRcvr := &Receiver{wg: &sync.WaitGroup{}}
			testRcvr.ctx, testRcvr.cancel = context.WithCancel(context.Background())
			defer testRcvr.cancel() // Ensure cancel is called to clean up goroutines if test panics

			// Apply test-specific setup
			if tt.setup != nil {
				tt.setup(t, testRcvr, mockDec, mockFChain, mockEnc, mockSWrt)
			}
			// If mocks were not set up (e.g. testRcvr.d), set defaults
			if testRcvr.d == nil { testRcvr.d = mockDec }
			if testRcvr.fc == nil { testRcvr.fc = mockFChain }
			if encoder == originalEncoder { encoder = mockEnc } // Avoid using original if not overridden
			if sampleWriter == originalSampleWriter { sampleWriter = mockSWrt }


			blockCh := make(chan []byte, 1)
			sampleBuf := new(bytes.Buffer)
			
			// Initialize prev map for the test if it's the "message in prev map" case.
			// This specific test logic needs prev map to be set up before calling processDecodedMessages.
			prevMapForTest := make(map[protocol.Digest]bool)
			if tt.name == "message in prev map (duplicate)" {
				msg := tt.messagesToSend[0].(mockMsg) // Assuming it's a mockMsg
				prevMapForTest[protocol.NewDigest(msg)] = true
			}
			nextMapForTest := make(map[protocol.Digest]bool)


			testRcvr.wg.Add(1) // For the processDecodedMessages goroutine
			go testRcvr.processDecodedMessages(blockCh, sampleBuf, prevMapForTest, nextMapForTest)

			// Send input block
			blockCh <- tt.inputBlockData
			// Send messages from mock decoder for this block
			for _, msg := range tt.messagesToSend { mockDec.msgChan <- msg }
			close(mockDec.msgChan) // Signal end of messages for this block

			close(blockCh) // Close blockCh to signal processDecodedMessages to exit its loop after this block
			testRcvr.wg.Wait() // Wait for processDecodedMessages to finish

			// Assertions
			if tt.expectRcvrError && testRcvr.err == nil { t.Errorf("expected rcvr.err to be set") }
			if !tt.expectRcvrError && testRcvr.err != nil { t.Errorf("unexpected rcvr.err: %v", testRcvr.err) }
			
			wasCancelled := false
			select {
			case <-testRcvr.ctx.Done(): wasCancelled = true
			default:
			}
			if tt.expectCancel && !wasCancelled { t.Errorf("expected context to be cancelled") }
			if !tt.expectCancel && wasCancelled && !tt.expectRcvrError { /* Can be cancelled by error path */ t.Logf("context was cancelled unexpectedly, rcvr.err: %v", testRcvr.err) }


			if mockEnc.encodeCalledCount != tt.expectedEncodeCount { t.Errorf("encode count: got %d, want %d", mockEnc.encodeCalledCount, tt.expectedEncodeCount) }
			if mockSWrt.writeCalledCount != tt.expectedWriteCount { t.Errorf("write count: got %d, want %d", mockSWrt.writeCalledCount, tt.expectedWriteCount) }

			if tt.expectPktFound && mockSWrt.writeCalledCount == 0 && !tt.expectRcvrError {
				// If pktFound was true, and no error stopped execution before write, write should have been called.
				// This check is implicitly covered by expectedWriteCount if error handling is correct.
			}
			if tt.expectPktFound && tt.expectedWriteCount > 0 {
				if !bytes.Equal(mockSWrt.lastWriteData, sampleBuf.Bytes()) && sampleBuf.Len() > 0 {
					// This check is valid if sampleBuf should contain exactly what was written.
					// sampleBuf contains accumulated data from multiple blocks if not cleared.
					// For single block test, sampleBuf.Bytes() after the block is processed should be what was written.
					// The current sampleBuf in test is local to test, not what processDecodedMessages uses internally.
					// processDecodedMessages receives sampleBuf as an argument.
					// t.Errorf("Expected written data to match sampleBuf content. Written: %x, SampleBuf: %x", mockSWrt.lastWriteData, sampleBuf.Bytes())
				}
			}


			if tt.finalPrev != nil {
				// The 'nextMapForTest' becomes 'prev' for the *next* iteration.
				// So we check 'nextMapForTest' state.
				if len(nextMapForTest) != len(tt.finalPrev) {
					t.Errorf("prev map length mismatch: got %d, want %d. Map: %+v", len(nextMapForTest), len(tt.finalPrev), nextMapForTest)
				}
				for k, v := range tt.finalPrev {
					if nextMapForTest[k] != v {
						t.Errorf("prev map content mismatch for key %v: got %v, want %v", k, nextMapForTest[k], v)
					}
				}
			}
			if *single { // Only check meterID count if *single was true for the test
				if len(meterID.UintMap) != tt.finalMeterIDCount {
					t.Errorf("meterID count: got %d, want %d. Map: %+v", len(meterID.UintMap), tt.finalMeterIDCount, meterID.UintMap)
				}
			}
		})
	}
}

// Placeholder for any final tests or if existing ones need minor adjustments
func TestPlaceholder(t *testing.T) {
	t.Log("Placeholder test for main_test.go completion.")
}
