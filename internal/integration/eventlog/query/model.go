package query

import "strings"

type SingleLimitResponse struct {
	Max       int `json:"Max"`
	Remaining int `json:"Remaining"`
}

type GenericEventResponse struct {
	TotalSize      int              `json:"totalSize"`
	Done           bool             `json:"done"`
	NextRecordsUrl string           `json:"nextRecordsUrl"`
	Records        []map[string]any `json:"records"`
}

type EventLogfileResponse struct {
	TotalSize      int                  `json:"totalSize"`
	Done           bool                 `json:"done"`
	NextRecordsUrl string               `json:"nextRecordsUrl"`
	Records        []EventLogfileRecord `json:"records"`
}

type EventLogfileRecord struct {
	Id          string `json:"Id"`
	LogDate     string `json:"LogDate"`
	CreatedDate string `json:"CreatedDate"`
	LogFile     string `json:"LogFile"`
	EventType   string `json:"EventType"`
	Interval    string `json:"Interval"`
	Sequence    int    `json:"Sequence"`
	// Salesforce types LogFileLength as a double, so the JSON is e.g. 2692.0.
	LogFileLength float64 `json:"LogFileLength"`
}

// SoqlQuery builds a SOQL statement with real spaces. Callers must URL-encode
// it (see queryURL); the previous form baked "+" into the string, which meant a
// literal "+" in a value reached Salesforce as a space and "&", "#" or "%"
// truncated or broke the query.
type SoqlQuery struct {
	fromTable   string
	selectAttrs []string
	where       string
	tail        string
}

func (s *SoqlQuery) AndWhere(where string) {
	if where == "" {
		return
	}
	if s.where == "" {
		s.where = where
	} else {
		s.where += " AND " + where
	}
}

func (s *SoqlQuery) OrWhere(where string) {
	if where == "" {
		return
	}
	if s.where == "" {
		s.where = where
	} else {
		s.where += " OR " + where
	}
}

func (s *SoqlQuery) AndOrWhere(where ...string) {
	if len(where) == 0 {
		return
	}
	resultWhere := "( " + strings.Join(where, " OR ") + " )"
	if s.where == "" {
		s.where = resultWhere
	} else {
		s.where += " AND " + resultWhere
	}
}

func (s *SoqlQuery) Tail(tail string) {
	if tail != "" {
		s.tail = " " + tail
	}
}

func (s *SoqlQuery) Build() string {
	soql := "SELECT " + strings.Join(s.selectAttrs, ",") + " FROM " + s.fromTable
	if s.where != "" {
		soql += " WHERE " + s.where
	}
	soql += s.tail
	return soql
}

func MakeSoqlQuery(fromTable string, selectAttrs ...string) SoqlQuery {
	return SoqlQuery{
		fromTable:   fromTable,
		selectAttrs: selectAttrs,
	}
}
