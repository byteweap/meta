package log

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFatalLogsBeforeExit(t *testing.T) {
	const child = "META_LOG_FATAL_TEST"
	if name := os.Getenv(child); name != "" {
		SetLogger(NewStdLogger(os.Stdout))
		helper := NewHelper(GetLogger())
		switch name {
		case "global":
			Fatal("fatal-global")
		case "globalf":
			Fatalf("fatal-%s", "globalf")
		case "globalw":
			Fatalw(DefaultMessageKey, "fatal-globalw")
		case "helper":
			helper.Fatal("fatal-helper")
		case "helperf":
			helper.Fatalf("fatal-%s", "helperf")
		case "helperw":
			helper.Fatalw(DefaultMessageKey, "fatal-helperw")
		default:
			t.Fatalf("unknown fatal logger case %q", name)
		}
		t.Fatal("fatal logger returned")
	}
	for _, name := range []string{"global", "globalf", "globalw", "helper", "helperf", "helperw"} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestFatalLogsBeforeExit$")
			cmd.Env = append(os.Environ(), child+"="+name)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, "%s", output)
			require.Equal(t, 1, exit.ExitCode(), "%s", output)
			require.Contains(t, string(output), "fatal-"+name)
			require.NotContains(t, string(output), "fatal logger returned")
		})
	}
}
