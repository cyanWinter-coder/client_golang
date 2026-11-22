// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promlint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/prometheus/client_golang/prometheus/testutil/promlint/validations"
)

// Rule is a stable identifier for a lint rule. Rule values are part of the
// public API and must not change between releases so that automated tooling
// can rely on them.
type Rule = validations.Rule

// Severity describes how strongly a finding affects the scanned metrics.
type Severity = validations.Severity

// The severities reported by Diagnostic.
const (
	SeverityInfo    = validations.SeverityInfo
	SeverityWarning = validations.SeverityWarning
	SeverityError   = validations.SeverityError
)

// The stable rule codes reported by Diagnostic. Each code identifies exactly
// one finding category so that callers such as automatic fixers can dispatch
// on the code instead of parsing the human-readable text.
const (
	// RuleHelp reports a metric family without help text.
	RuleHelp = validations.RuleHelp
	// RuleDuplicateHelp reports repeated or conflicting help text.
	RuleDuplicateHelp = validations.RuleDuplicateHelp
	// RuleDuplicateType reports repeated or conflicting type metadata.
	RuleDuplicateType = validations.RuleDuplicateType
	// RuleMetricUnits reports a metric name that uses a non-base unit.
	RuleMetricUnits = validations.RuleMetricUnits
	// RuleUnitAbbreviations reports a metric name that uses an abbreviated
	// unit.
	RuleUnitAbbreviations = validations.RuleUnitAbbreviations
	// RuleCounter reports counter metrics without the "_total" suffix.
	RuleCounter = validations.RuleCounter
	// RuleCounterReset reports non-counter metrics that carry the "_total"
	// suffix and would therefore be mistaken for counters.
	RuleCounterReset = validations.RuleCounterReset
	// RuleHistogramSummaryReserved reports names and labels reserved for
	// histograms and summaries.
	RuleHistogramSummaryReserved = validations.RuleHistogramSummaryReserved
	// RuleMetricTypeInName reports metric names that contain the metric type.
	RuleMetricTypeInName = validations.RuleMetricTypeInName
	// RuleReservedChars reports reserved characters in metric names.
	RuleReservedChars = validations.RuleReservedChars
	// RuleCamelCase reports camelCase metric or label names.
	RuleCamelCase = validations.RuleCamelCase
	// RuleDuplicateMetric reports duplicate series within a metric family.
	RuleDuplicateMetric = validations.RuleDuplicateMetric
	// RuleMalformedMetricFamily reports input that cannot be interpreted as a
	// metric family.
	RuleMalformedMetricFamily = validations.RuleMalformedMetricFamily
	// RuleCustom identifies findings produced by custom validations that do
	// not carry a rule of their own.
	RuleCustom = validations.RuleCustom
)

// Diagnostic is one structured finding of a metric specification scan.
type Diagnostic struct {
	// Metric is the name of the metric family the finding belongs to. It is
	// empty only if the input was so malformed that no metric name could be
	// determined.
	Metric string

	// Rule is the stable code of the rule that produced the finding.
	Rule Rule

	// Severity is the impact of the finding.
	Severity Severity

	// Path is the location of the finding within the metric family, for
	// example "name", "help", "type", "metric[3]", or
	// "metric[3].label[1].name". It is empty for findings that cannot be
	// attributed to a specific field.
	Path string

	// Line is the 1-based line number in the scanned text stream, or 0 if the
	// input was provided as metric family protos.
	Line int

	// Text is the human-readable description of the finding.
	Text string
}

// String implements the fmt.Stringer interface. It renders the finding in a
// single line suitable for logs.
func (d Diagnostic) String() string {
	loc := d.Metric
	if d.Path != "" {
		loc += ":" + d.Path
	}
	if d.Line > 0 {
		loc += ":" + strconv.Itoa(d.Line)
	}
	return fmt.Sprintf("%s: %s [%s/%s]", loc, d.Text, d.Rule, d.Severity)
}

// NewIssue returns an error that carries a rule code, severity, and field path.
// Custom validations registered with AddCustomValidations can return it from
// the []error result so that Scan reports the structured metadata instead of
// attributing the finding to RuleCustom.
//
// A Validation that returns a plain error keeps working unchanged.
func NewIssue(rule Rule, severity Severity, path, message string) error {
	return validations.NewIssue(rule, severity, path, message)
}

// NewIssuef is like NewIssue but formats the message with fmt.Sprintf.
func NewIssuef(rule Rule, severity Severity, path, format string, args ...any) error {
	return validations.NewIssuef(rule, severity, path, format, args...)
}

// Scan performs a linting pass over all metrics known to the linter and
// returns every finding, sorted deterministically by metric name, field path,
// rule, severity, line, and text. Unlike Lint, Scan does not stop at the first
// malformed metric: unparseable lines and panicking metric families are
// reported as RuleMalformedMetricFamily diagnostics while the remaining
// metrics are still scanned.
//
// If ctx is canceled or its deadline is exceeded, Scan stops as soon as it
// reaches the next cancellation point and returns the findings collected so
// far together with ctx.Err(). A nil ctx is treated as context.Background.
//
// Text input is buffered in memory so that the scan can resume after a
// malformed line. Scan reads the reader once; calling it after Lint, or twice
// on the same Linter, therefore scans only the remaining input.
//
// Scan is additive to Lint: the findings are derived from the same built-in
// and custom validations, so existing Lint output and Problem values are
// unaffected.
func (l *Linter) Scan(ctx context.Context) ([]Diagnostic, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var diagnostics []Diagnostic
	var families []*dto.MetricFamily

	if l.r != nil {
		data, err := readAllContext(ctx, l.r)
		if err != nil {
			return diagnostics, err
		}

		fams, diags, err := parseTextResilient(ctx, data)
		diagnostics = append(diagnostics, diags...)
		if err != nil {
			sortDiagnostics(diagnostics)
			return diagnostics, err
		}
		families = append(families, fams...)
	}

	families = append(families, l.mfs...)
	diagnostics = append(diagnostics, duplicateMetadataDiagnostics(families)...)

	for _, mf := range families {
		if err := ctx.Err(); err != nil {
			sortDiagnostics(diagnostics)
			return diagnostics, err
		}
		diagnostics = append(diagnostics, l.diagnoseResilient(mf)...)
	}

	diagnostics = dedupeDiagnostics(diagnostics)
	sortDiagnostics(diagnostics)
	return diagnostics, nil
}

// diagnoseResilient returns the diagnostics for a single metric family. A
// panic caused by a malformed family is turned into a diagnostic so that it
// cannot abort the scan of the remaining families.
func (l *Linter) diagnoseResilient(mf *dto.MetricFamily) (out []Diagnostic) {
	if mf == nil {
		return []Diagnostic{{
			Rule:     RuleMalformedMetricFamily,
			Severity: SeverityError,
			Text:     "nil metric family",
		}}
	}

	metric := mf.GetName()
	defer func() {
		if r := recover(); r != nil {
			out = append(out, Diagnostic{
				Metric:   metric,
				Rule:     RuleMalformedMetricFamily,
				Severity: SeverityError,
				Text:     fmt.Sprintf("panic while linting metric family: %v", r),
			})
		}
	}()
	return l.diagnose(mf)
}

// diagnose runs all validations for a single metric family and converts their
// errors into diagnostics.
func (l *Linter) diagnose(mf *dto.MetricFamily) []Diagnostic {
	var diagnostics []Diagnostic

	for _, fn := range defaultValidations {
		diagnostics = append(diagnostics, issuesToDiagnostics(mf.GetName(), fn(mf))...)
	}
	for _, fn := range l.customValidations {
		diagnostics = append(diagnostics, issuesToDiagnostics(mf.GetName(), fn(mf))...)
	}

	return diagnostics
}

// issuesToDiagnostics converts validation errors into diagnostics. Errors that
// carry a validations.Issue keep their rule, severity, and path; any other
// error is attributed to the custom rule.
func issuesToDiagnostics(metric string, errs []error) []Diagnostic {
	diagnostics := make([]Diagnostic, 0, len(errs))
	for _, err := range errs {
		d := Diagnostic{
			Metric:   metric,
			Rule:     RuleCustom,
			Severity: SeverityWarning,
			Text:     err.Error(),
		}

		if issue, ok := errors.AsType[*validations.Issue](err); ok {
			d.Rule = issue.Rule
			d.Severity = issue.Severity
			d.Path = issue.Path
			d.Text = issue.Message
		}
		diagnostics = append(diagnostics, d)
	}
	return diagnostics
}

// dedupeDiagnostics removes identical findings that were produced more than
// once, for example because a validation was registered twice or because a
// metric family appeared in both the text stream and the provided protos. The
// first occurrence is kept.
func dedupeDiagnostics(diagnostics []Diagnostic) []Diagnostic {
	seen := make(map[Diagnostic]struct{}, len(diagnostics))
	out := diagnostics[:0]
	for _, d := range diagnostics {
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}

// sortDiagnostics orders diagnostics by metric family, field path, rule,
// severity, line, and text. The ordering does not depend on the order in which
// metrics were decoded, so repeated scans of the same input produce identical
// results.
func sortDiagnostics(diagnostics []Diagnostic) {
	sort.SliceStable(diagnostics, func(i, j int) bool {
		a, b := diagnostics[i], diagnostics[j]
		if a.Metric != b.Metric {
			return a.Metric < b.Metric
		}
		if c := comparePath(a.Path, b.Path); c != 0 {
			return c < 0
		}
		if a.Rule != b.Rule {
			return a.Rule < b.Rule
		}
		if a.Severity != b.Severity {
			return a.Severity < b.Severity
		}
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Text < b.Text
	})
}

// comparePath compares two field paths segment by segment, comparing the
// indices of bracket segments numerically so that "metric[2]" sorts before
// "metric[10]".
func comparePath(a, b string) int {
	for a != "" && b != "" {
		sa, ra := nextPathSegment(a)
		sb, rb := nextPathSegment(b)
		if c := comparePathSegment(sa, sb); c != 0 {
			return c
		}
		a, b = ra, rb
	}
	switch {
	case a == "" && b == "":
		return 0
	case a == "":
		return -1
	default:
		return 1
	}
}

// nextPathSegment splits the first segment off a dotted field path.
func nextPathSegment(path string) (segment, rest string) {
	depth := 0
	for i := 0; i < len(path); i++ {
		switch path[i] {
		case '[':
			depth++
		case ']':
			depth--
		case '.':
			if depth == 0 {
				return path[:i], path[i+1:]
			}
		}
	}
	return path, ""
}

// comparePathSegment compares two path segments, ordering a trailing bracketed
// index numerically and everything else lexicographically.
func comparePathSegment(a, b string) int {
	na, ia, oka := splitPathIndex(a)
	nb, ib, okb := splitPathIndex(b)
	if oka && okb {
		if na != nb {
			return strings.Compare(na, nb)
		}
		if ia != ib {
			if ia < ib {
				return -1
			}
			return 1
		}
		return 0
	}
	return strings.Compare(a, b)
}

// splitPathIndex splits "name[7]" into "name" and 7.
func splitPathIndex(segment string) (name string, index int, ok bool) {
	open := strings.LastIndexByte(segment, '[')
	if open < 0 || !strings.HasSuffix(segment, "]") {
		return segment, 0, false
	}
	i, err := strconv.Atoi(segment[open+1 : len(segment)-1])
	if err != nil {
		return segment, 0, false
	}
	return segment[:open], i, true
}

// readAllContext reads r until EOF, aborting promptly when ctx is done.
func readAllContext(ctx context.Context, r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, ctxReader{ctx: ctx, r: r}); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ctxReader aborts reads as soon as the context is done.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

// Read implements the io.Reader interface.
func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// familyMetadata is the metadata collected from the comment lines of a text
// exposition stream.
type familyMetadata struct {
	help    string
	hasHelp bool
	typ     string
	hasType bool
}

// srcLine is a line of the input text together with its 1-based line number.
type srcLine struct {
	text   string
	number int
}

// parseTextResilient parses a text exposition stream, recovering from
// malformed lines so that the remaining metrics are still returned. Duplicate
// HELP and TYPE metadata is reported and removed before parsing because the
// underlying parser treats it as fatal.
//
// After an error the parser is restarted at the line following the offending
// one. Each restart reads the remaining lines lazily and the parser stops at
// the next error, so the total work stays linear in the number of lines.
func parseTextResilient(ctx context.Context, data []byte) ([]*dto.MetricFamily, []Diagnostic, error) {
	lines := splitLines(string(data))
	meta, diagnostics, lines := scanMetadata(lines)

	parsed := make(map[string]*dto.MetricFamily)

	for len(lines) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, diagnostics, err
		}

		parser := expfmt.NewTextParser(model.LegacyValidation)
		families, err := parser.TextToMetricFamilies(&lineReader{lines: lines})
		mergeFamilies(parsed, families)
		// The parser drops families that have metadata but no samples, so a
		// family whose samples follow a skipped line would lose its help text
		// and type. Restore both from the metadata seen earlier in the
		// stream.
		backfillMetadata(parsed, families, meta)
		if err == nil {
			break
		}

		var parseErr expfmt.ParseError
		if !errors.As(err, &parseErr) {
			diagnostics = append(diagnostics, Diagnostic{
				Rule:     RuleMalformedMetricFamily,
				Severity: SeverityError,
				Text:     err.Error(),
			})
			break
		}

		consumed := min(max(parseErr.Line, 1), len(lines))

		line := lines[consumed-1].number
		diagnostics = append(diagnostics, classifyParseError(parseErr, lines[consumed-1].text, line))

		lines = lines[consumed:]
	}

	return sortedFamilies(parsed), diagnostics, nil
}

// lineReader streams a slice of source lines as a text exposition, reading
// only as far as the parser needs. Unlike joining the lines into one string it
// does not copy the remaining input on every recovery step.
type lineReader struct {
	lines []srcLine
	index int
	off   int
}

// Read implements the io.Reader interface.
func (r *lineReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if r.index >= len(r.lines) {
			if n == 0 {
				return 0, io.EOF
			}
			return n, nil
		}
		line := r.lines[r.index].text
		if r.off < len(line) {
			c := copy(p[n:], line[r.off:])
			n += c
			r.off += c
			continue
		}
		p[n] = '\n'
		n++
		r.index++
		r.off = 0
	}
	return n, nil
}

// splitLines splits s into numbered lines. A trailing newline is appended if
// missing so that the text parser does not report a spurious premature end of
// input for a well-formed last line.
func splitLines(s string) []srcLine {
	if !strings.HasSuffix(s, "\n") {
		s += "\n"
	}
	texts := strings.Split(s, "\n")
	// The final element after the trailing newline is always empty.
	lines := make([]srcLine, 0, len(texts)-1)
	for i, text := range texts[:len(texts)-1] {
		lines = append(lines, srcLine{text: text, number: i + 1})
	}
	return lines
}

// backfillMetadata restores missing help text and type metadata on the
// families parsed from the current segment. A family whose metadata line was
// consumed by an earlier malformed segment is parsed as untyped without help,
// so the metadata seen at the start of the stream is reapplied.
func backfillMetadata(
	parsed, families map[string]*dto.MetricFamily,
	meta map[string]*familyMetadata,
) {
	for name := range families {
		fam := parsed[name]
		if fam == nil {
			continue
		}

		m := metadataFor(meta, name)
		if m == nil {
			continue
		}
		if fam.Help == nil && m.hasHelp {
			fam.Help = new(m.help)
		}
		// The parser defaults the type to untyped when the TYPE line is
		// missing, so an explicit declaration takes precedence.
		if fam.GetType() == dto.MetricType_UNTYPED && m.hasType {
			if typ, ok := parseMetricType(m.typ); ok {
				fam.Type = typ.Enum()
			}
		}
	}
}

// parseMetricType parses a TYPE value. Gauge histograms are accepted in both
// spellings because the text parser tolerates them.
func parseMetricType(value string) (dto.MetricType, bool) {
	typ := strings.ToUpper(value)
	if typ == "GAUGEHISTOGRAM" {
		return dto.MetricType_GAUGE_HISTOGRAM, true
	}
	if v, ok := dto.MetricType_value[typ]; ok {
		return dto.MetricType(v), true
	}
	return 0, false
}

// metadataFor returns the metadata for a parsed family. Histogram and summary
// samples are parsed into a family named after their base metric, so the
// suffixes that the parser recognizes are also consulted.
func metadataFor(meta map[string]*familyMetadata, name string) *familyMetadata {
	if m := meta[name]; m != nil {
		return m
	}
	for _, suffix := range []string{"_bucket", "_sum", "_count", "_created"} {
		if base, ok := strings.CutSuffix(name, suffix); ok {
			if m := meta[base]; m != nil && m.hasType {
				return m
			}
		}
	}
	return nil
}

// classifyParseError converts a text parsing error into a diagnostic with the
// most specific rule code that applies. The offending line is used to recover
// the metric name when the parser message does not include it.
func classifyParseError(parseErr expfmt.ParseError, text string, line int) Diagnostic {
	// Sample lines are the most reliable source of the metric name. Metadata
	// lines have no sample name, so fall back to the name quoted by the
	// parser in errors such as `second HELP line for metric name "x"`.
	name := sampleName(text)
	if name == "" {
		name = quotedName(parseErr.Msg)
	}

	d := Diagnostic{
		Metric:   name,
		Rule:     RuleMalformedMetricFamily,
		Severity: SeverityError,
		Line:     line,
		Text:     fmt.Sprintf("text format parsing error in line %d: %s", line, parseErr.Msg),
	}

	switch {
	case strings.HasPrefix(parseErr.Msg, "second HELP line for metric name"):
		d.Rule = RuleDuplicateHelp
		d.Path = "help"
		d.Text = fmt.Sprintf("second HELP line for metric name %q", name)
	case strings.HasPrefix(parseErr.Msg, "second TYPE line for metric name"):
		d.Rule = RuleDuplicateType
		d.Path = "type"
		d.Text = fmt.Sprintf("second TYPE line for metric name %q", name)
	}

	return d
}

// sampleName returns the metric name of a sample line, or an empty string if
// the line is a comment, blank, or metadata line.
func sampleName(line string) string {
	trimmed := strings.TrimLeft(line, " \t")
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return ""
	}
	if i := strings.IndexAny(trimmed, " \t{"); i >= 0 {
		return trimmed[:i]
	}
	return trimmed
}

// scanMetadata reports duplicate HELP and TYPE lines and removes them from the
// stream so that parsing can continue past them. The first declaration of each
// kind is kept.
func scanMetadata(lines []srcLine) (map[string]*familyMetadata, []Diagnostic, []srcLine) {
	meta := make(map[string]*familyMetadata)
	cleaned := make([]srcLine, 0, len(lines))
	var diagnostics []Diagnostic

	for _, line := range lines {
		kind, name, rest, ok := parseMetadataLine(line.text, "HELP")
		if !ok {
			kind, name, rest, ok = parseMetadataLine(line.text, "TYPE")
		}
		if !ok {
			cleaned = append(cleaned, line)
			continue
		}

		// The canonical parser silently ignores metadata without a value, so
		// treat such a line as undeclared rather than as a duplicate.
		if rest == "" {
			cleaned = append(cleaned, line)
			continue
		}
		if !validMetadata(kind, rest) {
			cleaned = append(cleaned, line)
			continue
		}

		m := meta[name]
		if m == nil {
			m = &familyMetadata{}
			meta[name] = m
		}

		switch kind {
		case "HELP":
			if m.hasHelp {
				diagnostics = append(diagnostics, Diagnostic{
					Metric:   name,
					Rule:     RuleDuplicateHelp,
					Severity: SeverityError,
					Path:     "help",
					Line:     line.number,
					Text:     fmt.Sprintf("second HELP line for metric name %q", name),
				})
				continue
			}
			m.hasHelp = true
			m.help = rest
		case "TYPE":
			if m.hasType {
				diagnostics = append(diagnostics, Diagnostic{
					Metric:   name,
					Rule:     RuleDuplicateType,
					Severity: SeverityError,
					Path:     "type",
					Line:     line.number,
					Text:     fmt.Sprintf("second TYPE line for metric name %q", name),
				})
				continue
			}
			m.hasType = true
			m.typ = rest
		}

		cleaned = append(cleaned, line)
	}

	return meta, diagnostics, cleaned
}

// parseMetadataLine parses a "# HELP" or "# TYPE" line for the given keyword.
// Quoted metric names are rejected so that the caller leaves them to the
// canonical parser.
func parseMetadataLine(line, keyword string) (kind, name, rest string, ok bool) {
	prefix := "# " + keyword
	if !strings.HasPrefix(line, prefix) || len(line) <= len(prefix) {
		return "", "", "", false
	}
	if line[len(prefix)] != ' ' && line[len(prefix)] != '\t' {
		return "", "", "", false
	}

	body := strings.TrimLeft(line[len(prefix):], " \t")
	if body == "" || strings.HasPrefix(body, `"`) {
		return "", "", "", false
	}

	i := strings.IndexAny(body, " \t")
	if i < 0 {
		return keyword, body, "", true
	}
	return keyword, body[:i], strings.TrimLeft(body[i:], " \t"), true
}

// validMetadata reports whether a non-empty metadata value is accepted by the
// canonical parser. TYPE values must name a known metric type; HELP values are
// accepted as-is.
func validMetadata(kind, value string) bool {
	if kind != "TYPE" {
		return true
	}
	_, ok := parseMetricType(value)
	return ok
}

// mergeFamilies merges src into dst, combining families that were decoded from
// different segments of the same stream.
func mergeFamilies(dst, src map[string]*dto.MetricFamily) {
	for name, fam := range src {
		existing, ok := dst[name]
		if !ok {
			dst[name] = fam
			continue
		}
		if existing.Help == nil && fam.Help != nil {
			existing.Help = fam.Help
		}
		if existing.Type == nil && fam.Type != nil {
			existing.Type = fam.Type
		}
		existing.Metric = append(existing.Metric, fam.GetMetric()...)
	}
}

// sortedFamilies returns the metric families of m ordered by name.
func sortedFamilies(m map[string]*dto.MetricFamily) []*dto.MetricFamily {
	families := make([]*dto.MetricFamily, 0, len(m))
	for _, name := range sortedKeys(m) {
		families = append(families, m[name])
	}
	return families
}

// duplicateMetadataDiagnostics reports metric families with the same name that
// carry conflicting help text or type metadata.
func duplicateMetadataDiagnostics(families []*dto.MetricFamily) []Diagnostic {
	type seen struct {
		help    string
		hasHelp bool
		typ     dto.MetricType
		hasType bool
	}

	first := make(map[string]seen, len(families))
	var diagnostics []Diagnostic

	for _, mf := range families {
		if mf == nil {
			continue
		}
		name := mf.GetName()
		s, ok := first[name]
		if !ok {
			first[name] = seen{
				help:    mf.GetHelp(),
				hasHelp: mf.Help != nil,
				typ:     mf.GetType(),
				hasType: mf.Type != nil,
			}
			continue
		}

		if mf.Help != nil && s.hasHelp && mf.GetHelp() != s.help {
			diagnostics = append(diagnostics, Diagnostic{
				Metric:   name,
				Rule:     RuleDuplicateHelp,
				Severity: SeverityError,
				Path:     "help",
				Text:     fmt.Sprintf("conflicting help text for metric family %q", name),
			})
		}
		if mf.Type != nil && s.hasType && mf.GetType() != s.typ {
			diagnostics = append(diagnostics, Diagnostic{
				Metric:   name,
				Rule:     RuleDuplicateType,
				Severity: SeverityError,
				Path:     "type",
				Text:     fmt.Sprintf("conflicting type metadata for metric family %q", name),
			})
		}
	}

	return diagnostics
}

// quotedName extracts and unquotes the first quoted name found in msg.
func quotedName(msg string) string {
	start := strings.IndexByte(msg, '"')
	if start < 0 {
		return ""
	}
	for i := start + 1; i < len(msg); i++ {
		switch msg[i] {
		case '\\':
			i++
		case '"':
			if name, err := strconv.Unquote(msg[start : i+1]); err == nil {
				return name
			}
			return msg[start+1 : i]
		}
	}
	return ""
}

// sortedKeys returns the keys of m in ascending order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
