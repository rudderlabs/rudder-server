package testjson

import (
	"fmt"
	"io"
	"strings"

	"github.com/fatih/color"
)

func debugFormat(event TestEvent, _ *Execution) string {
	return fmt.Sprintf("%s %s %s (%.3f) [%d] %s\n",
		event.Package,
		event.Test,
		event.Action,
		event.Elapsed,
		event.Time.Unix(),
		event.Output)
}

// go test -v
func standardVerboseFormat(event TestEvent, _ *Execution) string {
	if event.Action == ActionOutput {
		return event.Output
	}
	return ""
}

// go test
func standardQuietFormat(event TestEvent, _ *Execution) string {
	if !event.PackageEvent() {
		return ""
	}
	if event.Output == "PASS\n" || isCoverageOutput(event.Output) {
		return ""
	}
	if isWarningNoTestsToRunOutput(event.Output) {
		return ""
	}

	return event.Output
}

func testNameFormat(event TestEvent, exec *Execution) string {
	result := colorEvent(event)(strings.ToUpper(string(event.Action)))
	formatTest := func() string {
		pkgPath := RelativePackagePath(event.Package)

		return fmt.Sprintf("%s %s%s %s\n",
			result,
			joinPkgToTestName(pkgPath, event.Test),
			formatRunID(event.RunID),
			event.ElapsedFormatted())
	}

	switch {
	case isPkgFailureOutput(event):
		return event.Output

	case event.PackageEvent():
		if !event.Action.IsTerminal() {
			return ""
		}
		pkg := exec.Package(event.Package)
		if event.Action == ActionSkip || (event.Action == ActionPass && pkg.Total == 0) {
			result = colorEvent(event)("EMPTY")
		}

		event.Elapsed = 0 // hide elapsed for now, for backwards compat
		return result + " " + packageLine(event, exec)

	case event.Action == ActionFail:
		pkg := exec.Package(event.Package)
		tc := pkg.LastFailedByName(event.Test)
		return pkg.Output(tc.ID) + formatTest()

	case event.Action == ActionPass:
		return formatTest()
	}
	return ""
}

// joinPkgToTestName for formatting.
// If the package path isn't the current directory, we add a period to separate
// the test name and the package path. If it is the current directory, we don't
// show it at all. This prevents output like ..MyTest when the test is in the
// current directory.
func joinPkgToTestName(pkg string, test string) string {
	if pkg == "." {
		return test
	}
	return pkg + "." + test
}

// formatRunID returns a formatted string of the runID.
func formatRunID(runID int) string {
	if runID <= 0 {
		return ""
	}
	return fmt.Sprintf(" (re-run %d)", runID)
}

// isPkgFailureOutput returns true if the event is package output, and the output
// doesn't match any of the expected framing messages. Events which match this
// pattern should be package-level failures (ex: exit(1) or panic in an init() or
// TestMain).
func isPkgFailureOutput(event TestEvent) bool {
	out := event.Output
	return all(
		event.PackageEvent(),
		event.Action == ActionOutput,
		out != "PASS\n",
		out != "FAIL\n",
		!isWarningNoTestsToRunOutput(out),
		!strings.HasPrefix(out, "FAIL\t"+event.Package),
		!strings.HasPrefix(out, "ok  \t"+event.Package),
		!strings.HasPrefix(out, "?   \t"+event.Package),
		!isShuffleSeedOutput(out),
	)
}

func all(cond ...bool) bool {
	for _, c := range cond {
		if !c {
			return false
		}
	}
	return true
}

func pkgNameFormat(event TestEvent, exec *Execution) string {
	if !event.PackageEvent() {
		return ""
	}
	return shortFormatPackageEvent(event, exec)
}

func shortFormatPackageEvent(event TestEvent, exec *Execution) string {
	pkg := exec.Package(event.Package)

	fmtEvent := func(action string) string {
		return action + "  " + packageLine(event, exec)
	}
	withColor := colorEvent(event)
	switch event.Action {
	case ActionSkip:
		return fmtEvent(withColor("∅"))
	case ActionPass:
		if pkg.Total == 0 {
			return fmtEvent(withColor("∅"))
		}
		return fmtEvent(withColor("✓"))
	case ActionFail:
		return fmtEvent(withColor("✖"))
	}
	return ""
}

func packageLine(event TestEvent, exec *Execution) string {
	pkg := exec.Package(event.Package)

	var buf strings.Builder
	buf.WriteString(RelativePackagePath(event.Package))

	switch {
	case pkg.cached:
		buf.WriteString(" (cached)")
	case event.Elapsed != 0:
		d := elapsedDuration(event.Elapsed)
		buf.WriteString(fmt.Sprintf(" (%s)", d))
	}

	if pkg.coverage != "" {
		buf.WriteString(" (" + pkg.coverage + ")")
	}

	if event.Action == ActionFail && pkg.shuffleSeed != "" {
		buf.WriteString(" (" + pkg.shuffleSeed + ")")
	}
	buf.WriteString("\n")
	return buf.String()
}

func pkgNameWithFailuresFormat(event TestEvent, exec *Execution) string {
	if !event.PackageEvent() {
		if event.Action == ActionFail {
			pkg := exec.Package(event.Package)
			tc := pkg.LastFailedByName(event.Test)
			return pkg.Output(tc.ID)
		}
		return ""
	}
	return shortFormatPackageEvent(event, exec)
}

func colorEvent(event TestEvent) func(format string, a ...interface{}) string {
	switch event.Action {
	case ActionPass:
		return color.GreenString
	case ActionFail:
		return color.RedString
	case ActionSkip:
		return color.YellowString
	}
	return color.WhiteString
}

// EventFormatter is a function which handles an event and returns a string to
// output for the event.
type EventFormatter interface {
	Format(event TestEvent, output *Execution) error
}

// NewEventFormatter returns a formatter for printing events.
func NewEventFormatter(out io.Writer, format string) EventFormatter {
	switch format {
	case "debug":
		return &formatAdapter{out, debugFormat}
	case "standard-verbose":
		return &formatAdapter{out, standardVerboseFormat}
	case "standard-quiet":
		return &formatAdapter{out, standardQuietFormat}
	case "dots", "dots-v1":
		return &formatAdapter{out, dotsFormatV1}
	case "dots-v2":
		return newDotFormatter(out)
	case "testname", "short-verbose":
		return &formatAdapter{out, testNameFormat}
	case "pkgname", "short":
		return &formatAdapter{out, pkgNameFormat}
	case "pkgname-and-test-fails", "short-with-failures":
		return &formatAdapter{out, pkgNameWithFailuresFormat}
	default:
		return nil
	}
}

type formatAdapter struct {
	out    io.Writer
	format func(TestEvent, *Execution) string
}

func (f *formatAdapter) Format(event TestEvent, exec *Execution) error {
	o := f.format(event, exec)
	_, err := f.out.Write([]byte(o))
	return err
}
