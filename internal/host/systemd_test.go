package host

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyntheticUnitDraftOmitsUnreviewedData(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", "examples", "host-observation", "unit.show"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	unit, err := ReadUnit("qwen.service", f)
	if err != nil {
		t.Fatal(err)
	}
	result, err := json.Marshal(unit.Draft())
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{"synthetic-secret", "ExecStart", "Environment", "/opt/example", "startup_peak_bytes"} {
		if strings.Contains(string(result), needle) {
			t.Fatalf("unexpected draft value %q", needle)
		}
	}
	if unit.ActiveState != "active" || unit.MemoryCurrent == nil || *unit.MemoryCurrent != 32<<30 || !unit.MemoryMaxUnlimited || len(unit.Draft().RequiredInputs) == 0 {
		t.Fatalf("unexpected observation: %+v", unit)
	}
}

func TestUnitRejectsNonLiteralNames(t *testing.T) {
	for _, name := range []string{"*.service", "--all.service", "example.timer", "foo bar.service", "../foo.service", "foo\n.service", "foo.service\n"} {
		if ValidService(name) {
			t.Fatalf("accepted %q", name)
		}
	}
	if !ValidService("foo@bar.service") {
		t.Fatal("instance unit rejected")
	}
}

func TestUnitRejectsUnexpectedValuesWithoutEcho(t *testing.T) {
	for _, extra := range []string{"ActiveState=synthetic-secret", "MemoryMax=synthetic-secret", "UnitFileState=synthetic-secret"} {
		_, err := ReadUnit("example.service", strings.NewReader("LoadState=loaded\nActiveState=active\n"+extra+"\n"))
		if err == nil || strings.Contains(err.Error(), "synthetic-secret") {
			t.Fatalf("invalid diagnostics: %v", err)
		}
	}
}

func TestValidUnitAcceptsLiteralServicesAndTimersOnly(t *testing.T) {
	for _, ok := range []string{"a.service", "canary.timer", "foo@bar.service", "a-b_c.timer"} {
		if !ValidUnit(ok) {
			t.Errorf("%q rejected", ok)
		}
	}
	for _, bad := range []string{"", "*.timer", "a.socket", "a b.timer", "../x.timer", "-x.timer", "a.timer; reboot", "a.service\n"} {
		if ValidUnit(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
	if ValidService("canary.timer") {
		t.Error("ValidService must stay service-only")
	}
}
