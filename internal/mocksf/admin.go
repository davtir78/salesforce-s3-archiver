package mocksf

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/log"
)

// Ledger lists everything the mock has generated, for reconciliation.
type Ledger struct {
	OrgId string `json:"orgId"`
	// topic -> EventIdentifier values
	Streams map[string][]string `json:"streams"`
	// EventLogFile Id -> REQUEST_ID values
	EventLogFiles map[string][]string `json:"eventLogFiles"`
	// object -> record Ids
	CustomRecords map[string][]string `json:"customRecords"`
}

func (s *Server) Ledger() Ledger {
	l := Ledger{OrgId: s.opts.OrgId, Streams: map[string][]string{}, EventLogFiles: map[string][]string{}, CustomRecords: map[string][]string{}}
	for name, t := range s.topics {
		t.mu.Lock()
		l.Streams[name] = append([]string(nil), t.ledger...)
		t.mu.Unlock()
	}
	s.mu.Lock()
	for id, reqs := range s.elfLedger {
		l.EventLogFiles[id] = append([]string(nil), reqs...)
	}
	for obj, ids := range s.customLedger {
		l.CustomRecords[obj] = append([]string(nil), ids...)
	}
	s.mu.Unlock()
	return l
}

// GeneratorConfig controls background data generation.
type GeneratorConfig struct {
	// Events published per topic per second.
	EventsPerSecond int `json:"eventsPerSecond"`
	// Seconds between generated EventLogFiles (per event type).
	EventLogFileEverySeconds int `json:"eventLogFileEverySeconds"`
	// Seconds between generated custom query rows (per object).
	CustomRecordEverySeconds int `json:"customRecordEverySeconds"`
}

// StartGenerator starts background generation, replacing any running generator.
func (s *Server) StartGenerator(cfg GeneratorConfig) {
	s.StopGenerator()
	stop := make(chan struct{})
	s.mu.Lock()
	s.stopGen = stop
	s.mu.Unlock()

	run := func(every time.Duration, fn func()) {
		if every <= 0 {
			return
		}
		s.genWG.Add(1)
		go func() {
			defer s.genWG.Done()
			ticker := time.NewTicker(every)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					fn()
				}
			}
		}()
	}

	if cfg.EventsPerSecond > 0 {
		// Publish in 100ms slices for a smooth rate.
		perTick := cfg.EventsPerSecond / 10
		every := 100 * time.Millisecond
		if perTick == 0 {
			perTick = 1
			every = time.Second / time.Duration(cfg.EventsPerSecond)
		}
		run(every, func() {
			for name := range s.topics {
				if _, err := s.PublishEvents(name, perTick); err != nil {
					log.Errorf("mock generator publish: %v", err)
				}
			}
		})
	}
	run(time.Duration(cfg.EventLogFileEverySeconds)*time.Second, func() {
		for _, et := range s.opts.EventLogTypes {
			s.AddEventLogFile(et, time.Now(), s.opts.EventLogRows)
		}
	})
	run(time.Duration(cfg.CustomRecordEverySeconds)*time.Second, func() {
		for _, obj := range s.opts.CustomObjects {
			s.AddCustomRecords(obj, time.Now(), 3)
		}
	})
}

func (s *Server) StopGenerator() {
	s.mu.Lock()
	stop := s.stopGen
	s.stopGen = nil
	s.mu.Unlock()
	if stop != nil {
		close(stop)
		s.genWG.Wait()
	}
}

var adminMu sync.Mutex

func (s *Server) registerAdmin(mux *http.ServeMux) {
	mux.HandleFunc("/admin/ledger", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.Ledger())
	})
	mux.HandleFunc("/admin/publish", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("count"))
		if n <= 0 {
			n = 1
		}
		topics := r.URL.Query()["topic"]
		if len(topics) == 0 {
			topics = s.opts.Topics
		}
		total := 0
		for _, t := range topics {
			ids, err := s.PublishEvents(t, n)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			total += len(ids)
		}
		writeJSON(w, http.StatusOK, map[string]int{"published": total})
	})
	mux.HandleFunc("/admin/eventlogfile", func(w http.ResponseWriter, r *http.Request) {
		rows, _ := strconv.Atoi(r.URL.Query().Get("rows"))
		if rows <= 0 {
			rows = s.opts.EventLogRows
		}
		et := r.URL.Query().Get("eventType")
		if et == "" {
			et = s.opts.EventLogTypes[0]
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": s.AddEventLogFile(et, time.Now(), rows)})
	})
	mux.HandleFunc("/admin/faults", func(w http.ResponseWriter, r *http.Request) {
		adminMu.Lock()
		defer adminMu.Unlock()
		if r.Method == http.MethodPost {
			var f Faults
			if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			s.SetFaults(f)
		}
		writeJSON(w, http.StatusOK, s.GetFaults())
	})
	mux.HandleFunc("/admin/expire-tokens", func(w http.ResponseWriter, r *http.Request) {
		s.ExpireTokens()
		writeJSON(w, http.StatusOK, map[string]bool{"expired": true})
	})
	mux.HandleFunc("/admin/generator", func(w http.ResponseWriter, r *http.Request) {
		adminMu.Lock()
		defer adminMu.Unlock()
		var cfg GeneratorConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if cfg.EventsPerSecond == 0 && cfg.EventLogFileEverySeconds == 0 && cfg.CustomRecordEverySeconds == 0 {
			s.StopGenerator()
		} else {
			s.StartGenerator(cfg)
		}
		writeJSON(w, http.StatusOK, cfg)
	})
	mux.HandleFunc("/admin/stats", func(w http.ResponseWriter, r *http.Request) {
		l := s.Ledger()
		stats := map[string]int{}
		for t, ids := range l.Streams {
			stats["stream:"+t] = len(ids)
		}
		rows := 0
		for _, ids := range l.EventLogFiles {
			rows += len(ids)
		}
		stats["eventLogFiles"] = len(l.EventLogFiles)
		stats["eventLogRows"] = rows
		for obj, ids := range l.CustomRecords {
			stats["custom:"+obj] = len(ids)
		}
		writeJSON(w, http.StatusOK, stats)
	})
}
