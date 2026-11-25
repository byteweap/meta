package zerolog

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/byteweap/meta/component/log"
)

// testWriteSyncer 测试写入器
type testWriteSyncer struct {
	output []string
}

// Write 写入数据
func (x *testWriteSyncer) Write(p []byte) (n int, err error) {
	x.output = append(x.output, string(p))
	return len(p), nil
}

// TestLogger 测试日志记录器
func TestLogger(t *testing.T) {
	syncer := &testWriteSyncer{}
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	zlogger := zerolog.New(syncer)
	logger := NewLogger(&zlogger)

	klog := log.NewHelper(logger)

	klog.Debugw("log", "debug")
	klog.Infow("log", "info")
	klog.Warnw("log", "warn")
	klog.Errorw("log", "error")

	except := []string{
		"{\"level\":\"debug\",\"log\":\"debug\"}\n",
		"{\"level\":\"info\",\"log\":\"info\"}\n",
		"{\"level\":\"warn\",\"log\":\"warn\"}\n",
		"{\"level\":\"error\",\"log\":\"error\"}\n",
	}
	for i, s := range except {
		if s != syncer.output[i] {
			t.Logf("except=%s, got=%s", s, syncer.output[i])
			t.Fail()
		}
	}
}

func TestLoggerHandlesEmptyAndUnpairedFields(t *testing.T) {
	syncer := &testWriteSyncer{}
	backend := zerolog.New(syncer)
	logger := NewLogger(&backend)
	if err := logger.Log(log.LevelInfo); err != nil || len(syncer.output) != 0 {
		t.Fatalf("empty log returned %v, output=%v", err, syncer.output)
	}
	if err := logger.Log(log.Level(100), 1, "discard", "unpaired"); err != nil {
		t.Fatal(err)
	}
	want := "{\"level\":\"debug\",\"unpaired\":\"\"}\n"
	if len(syncer.output) != 1 || syncer.output[0] != want {
		t.Fatalf("log output = %v, want %s", syncer.output, want)
	}
}

func TestFatalLogTerminatesAfterWriting(t *testing.T) {
	const child = "META_ZEROLOG_FATAL_TEST"
	if os.Getenv(child) == "1" {
		backend := zerolog.New(os.Stdout)
		_ = NewLogger(&backend).Log(log.LevelFatal, "field", "fatal-payload")
		t.Fatal("fatal logger returned")
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestFatalLogTerminatesAfterWriting$")
	cmd.Env = append(os.Environ(), child+"=1")
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(output), `"level":"fatal","field":"fatal-payload"`) ||
		strings.Contains(string(output), "fatal logger returned") {
		t.Fatalf("fatal logger returned err=%v output=%q", err, output)
	}
}
