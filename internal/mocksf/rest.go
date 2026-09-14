package mocksf

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const sfDateFormat = "2006-01-02T15:04:05.000+0000"

type eventLogFile struct {
	Id          string
	EventType   string
	CreatedDate time.Time
	LogDate     time.Time
	Interval    string
	Sequence    int
	csv         []byte
}

type queryCursor struct {
	records []map[string]any
	offset  int
}

var (
	reFrom   = regexp.MustCompile(`(?i)\bFROM\s+(\w+)`)
	reGTE    = regexp.MustCompile(`(\w+)\s*>=\s*([0-9T:\-+.Z]+)`)
	reLTE    = regexp.MustCompile(`(\w+)\s*<=\s*([0-9T:\-+.Z]+)`)
	reType   = regexp.MustCompile(`EventType\s*=\s*'(\w+)'`)
	reOrder  = regexp.MustCompile(`(?i)ORDER\s+BY\s+(\w+)`)
	reSelect = regexp.MustCompile(`(?i)^\s*SELECT\s+(.+?)\s+FROM\s`)
)

// AddEventLogFile generates an EventLogFile with rows CSV lines.
func (s *Server) AddEventLogFile(eventType string, created time.Time, rows int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := fmt.Sprintf("0AT%012d", len(s.elfs)+1)
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Write([]string{"EVENT_TYPE", "TIMESTAMP", "REQUEST_ID", "ORGANIZATION_ID", "USER_ID", "RUN_TIME", "CLIENT_IP", "URI", "LOGIN_TYPE"})
	reqIds := make([]string, 0, rows)
	for i := 0; i < rows; i++ {
		reqId := fmt.Sprintf("%s-%s-%06d", id, strings.ToLower(eventType), i)
		ts := created.Add(-time.Duration(rows-i) * time.Second).UTC()
		w.Write([]string{
			eventType,
			ts.Format("20060102150405.000"),
			reqId,
			s.opts.OrgId,
			s.opts.UserId,
			strconv.Itoa(10 + i%500),
			fmt.Sprintf("10.1.%d.%d", i%250, (i/250)%250),
			"/lightning/r/Account/001MOCK" + strconv.Itoa(i),
			"Application, with \"quotes\"",
		})
		reqIds = append(reqIds, reqId)
	}
	w.Flush()
	s.elfs = append(s.elfs, &eventLogFile{
		Id:          id,
		EventType:   eventType,
		CreatedDate: created.UTC(),
		LogDate:     created.UTC().Truncate(time.Hour).Add(-time.Hour),
		Interval:    "Hourly",
		Sequence:    1,
		csv:         buf.Bytes(),
	})
	s.elfLedger[id] = reqIds
	return id
}

// AddCustomRecords generates n rows for a custom query object.
func (s *Server) AddCustomRecords(object string, created time.Time, n int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("0YM%012d", len(s.customLedger[object])+1)
		row := map[string]any{
			"attributes":     map[string]any{"type": object, "url": "/services/data/v64.0/sobjects/" + object + "/" + id},
			"Id":             id,
			"Action":         "changedApexClass",
			"Section":        "Apex Class",
			"CreatedDate":    created.UTC().Format(sfDateFormat),
			"SystemModstamp": created.UTC().Format(sfDateFormat),
			"CreatedById":    s.opts.UserId,
		}
		s.custom[object] = append(s.custom[object], row)
		s.customLedger[object] = append(s.customLedger[object], id)
		ids = append(ids, id)
	}
	return ids
}

func (s *Server) takeRestFault() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.faults.FailRestRequests > 0 {
		s.faults.FailRestRequests--
		return true
	}
	return false
}

func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	if !s.validToken(bearer(r)) {
		writeJSON(w, http.StatusUnauthorized, []map[string]string{{"errorCode": "INVALID_SESSION_ID", "message": "Session expired or invalid"}})
		return
	}
	if s.takeRestFault() {
		writeJSON(w, http.StatusInternalServerError, []map[string]string{{"errorCode": "UNKNOWN_EXCEPTION", "message": "injected fault"}})
		return
	}
	// /services/data/vXX.X/<rest>
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/services/data/"), "/", 2)
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	version, rest := parts[0], parts[1]
	switch {
	case rest == "query" || rest == "tooling/query":
		s.handleQuery(w, r, version)
	case strings.HasPrefix(rest, "query/"):
		s.handleQueryMore(w, strings.TrimPrefix(rest, "query/"), version)
	case strings.HasPrefix(rest, "sobjects/EventLogFile/") && strings.HasSuffix(rest, "/LogFile"):
		id := strings.TrimSuffix(strings.TrimPrefix(rest, "sobjects/EventLogFile/"), "/LogFile")
		s.handleLogFile(w, id)
	case rest == "limits":
		writeJSON(w, http.StatusOK, map[string]map[string]int{
			"DailyApiRequests":        {"Max": 1000000, "Remaining": 999000},
			"DailyStreamingApiEvents": {"Max": 1000000, "Remaining": 998000},
			"PermissionSets":          {"Max": 1500, "Remaining": 1400},
		})
	default:
		http.NotFound(w, r)
	}
}

func parseSFTime(v string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, sfDateFormat, "2006-01-02T15:04:05.999999-0700"} {
		if t, err := time.Parse(layout, v); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("bad datetime %q", v)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request, version string) {
	q := r.URL.Query().Get("q")
	from := reFrom.FindStringSubmatch(q)
	if from == nil {
		writeJSON(w, http.StatusBadRequest, []map[string]string{{"errorCode": "MALFORMED_QUERY", "message": "missing FROM"}})
		return
	}
	var since, until *time.Time
	var field string
	if m := reGTE.FindStringSubmatch(q); m != nil {
		t, err := parseSFTime(m[2])
		if err != nil {
			writeJSON(w, http.StatusBadRequest, []map[string]string{{"errorCode": "MALFORMED_QUERY", "message": err.Error()}})
			return
		}
		since, field = &t, m[1]
	}
	if m := reLTE.FindStringSubmatch(q); m != nil {
		t, err := parseSFTime(m[2])
		if err != nil {
			writeJSON(w, http.StatusBadRequest, []map[string]string{{"errorCode": "MALFORMED_QUERY", "message": err.Error()}})
			return
		}
		until = &t
		if field == "" {
			field = m[1]
		}
	}
	inRange := func(t time.Time) bool {
		return (since == nil || !t.Before(*since)) && (until == nil || !t.After(*until))
	}

	var records []map[string]any
	s.mu.Lock()
	if strings.EqualFold(from[1], "EventLogFile") {
		types := map[string]bool{}
		for _, m := range reType.FindAllStringSubmatch(q, -1) {
			types[m[1]] = true
		}
		for _, f := range s.elfs {
			if len(types) > 0 && !types[f.EventType] {
				continue
			}
			if !inRange(f.CreatedDate) {
				continue
			}
			records = append(records, map[string]any{
				"attributes":    map[string]any{"type": "EventLogFile", "url": "/services/data/" + version + "/sobjects/EventLogFile/" + f.Id},
				"Id":            f.Id,
				"EventType":     f.EventType,
				"CreatedDate":   f.CreatedDate.Format(sfDateFormat),
				"LogDate":       f.LogDate.Format(sfDateFormat),
				"Interval":      f.Interval,
				"Sequence":      f.Sequence,
				"LogFileLength": len(f.csv),
				"LogFile":       "/services/data/" + version + "/sobjects/EventLogFile/" + f.Id + "/LogFile",
			})
		}
	} else {
		for _, row := range s.custom[from[1]] {
			ts := row["CreatedDate"].(string)
			if field != "" {
				if v, ok := row[field].(string); ok {
					ts = v
				}
			}
			t, _ := parseSFTime(ts)
			if !inRange(t) {
				continue
			}
			records = append(records, filterFields(row, q))
		}
	}
	s.mu.Unlock()

	if m := reOrder.FindStringSubmatch(q); m != nil {
		key := m[1]
		sort.SliceStable(records, func(i, j int) bool {
			return fmt.Sprint(records[i][key]) < fmt.Sprint(records[j][key])
		})
	}
	s.respondPage(w, records, 0, version)
}

func filterFields(row map[string]any, q string) map[string]any {
	m := reSelect.FindStringSubmatch(q)
	if m == nil {
		return row
	}
	out := map[string]any{"attributes": row["attributes"]}
	for _, f := range strings.Split(m[1], ",") {
		f = strings.TrimSpace(f)
		if v, ok := row[f]; ok {
			out[f] = v
		}
	}
	return out
}

func (s *Server) respondPage(w http.ResponseWriter, records []map[string]any, offset int, version string) {
	end := offset + s.opts.QueryPageSize
	if end > len(records) {
		end = len(records)
	}
	resp := map[string]any{
		"totalSize": len(records),
		"done":      end >= len(records),
		"records":   records[offset:end],
	}
	if records == nil {
		resp["records"] = []any{}
	}
	if end < len(records) {
		cursor := "01gMOCK" + randomHex(6) + "-" + strconv.Itoa(end)
		s.mu.Lock()
		s.queryCursors[cursor] = queryCursor{records: records, offset: end}
		s.mu.Unlock()
		resp["nextRecordsUrl"] = "/services/data/" + version + "/query/" + cursor
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleQueryMore(w http.ResponseWriter, cursor, version string) {
	s.mu.Lock()
	c, ok := s.queryCursors[cursor]
	delete(s.queryCursors, cursor)
	s.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusBadRequest, []map[string]string{{"errorCode": "INVALID_QUERY_LOCATOR", "message": "invalid query locator"}})
		return
	}
	s.respondPage(w, c.records, c.offset, version)
}

func (s *Server) handleLogFile(w http.ResponseWriter, id string) {
	s.mu.Lock()
	var file *eventLogFile
	for _, f := range s.elfs {
		if f.Id == id {
			file = f
		}
	}
	truncate := s.faults.TruncateNextDownload
	if file != nil && truncate > 0 {
		s.faults.TruncateNextDownload = 0
	}
	s.mu.Unlock()
	if file == nil {
		writeJSON(w, http.StatusNotFound, []map[string]string{{"errorCode": "NOT_FOUND", "message": "not found"}})
		return
	}
	body := file.csv
	w.Header().Set("Content-Type", "text/csv")
	if truncate > 0 && truncate < len(body) {
		// Advertise the full length but send less, so the client sees an unexpected EOF.
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body[:truncate])
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
			}
		}
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Write(body)
}
