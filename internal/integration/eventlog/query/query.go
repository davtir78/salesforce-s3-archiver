package query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/cache"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/davtir78/salesforce-s3-archiver/internal/oauth"
)

// Safety limit on followed nextRecordsUrl pages per query.
const maxPages = 10000

func auth(conf *config.EventLogConfig, db cache.Cache) (string, error) {
	accessToken, ok := getTokenFromCache(conf, db).(string)
	if ok {
		log.Debugf("Got token from cache")
		return accessToken, nil
	}
	log.Debugf("No token in cache, send login request")
	login, err := oauth.Login(conf.Auth)
	if err != nil {
		return "", err
	}
	setTokenIntoCache(conf, db, login.AccessToken)
	return login.AccessToken, nil
}

func relogin(conf *config.EventLogConfig, db cache.Cache) error {
	deleteTokenFromCache(conf, db)
	_, reqErr := auth(conf, db)
	return reqErr
}

func getTokenFromCache(conf *config.EventLogConfig, db cache.Cache) any {
	val, err := db.GetCacheVal(tokenCacheKey(conf))
	if err != nil {
		log.Warnf("Error getting token from cache: %s", err.Error())
	}
	return val
}

func setTokenIntoCache(conf *config.EventLogConfig, db cache.Cache, accessToken string) {
	if err := db.SetCacheVal(tokenCacheKey(conf), accessToken); err != nil {
		log.Warnf("Error setting token into cache: %s", err.Error())
	}
}

func deleteTokenFromCache(conf *config.EventLogConfig, db cache.Cache) {
	if err := db.DelCacheVal(tokenCacheKey(conf)); err != nil {
		log.Warnf("Error deleting token from cache: %s", err.Error())
	}
}

func tokenCacheKey(conf *config.EventLogConfig) string {
	return conf.Name + "_access_token"
}

// HTTPError carries the status code so callers can tell a permanent rejection
// (a 4xx that will never succeed) from a transient failure.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("Error %d, body: %s", e.StatusCode, e.Body)
}

// Permanent reports whether retrying the same request is pointless. It is an
// allowlist: a permanent error lets a file be tombstoned and skipped, so any
// status that can recover must stay transient. 401 is handled by re-login,
// 429 is rate limiting, and 403 covers REQUEST_LIMIT_EXCEEDED (resets within
// 24 hours) as well as permission errors an admin can fix.
func (e *HTTPError) Permanent() bool {
	switch e.StatusCode {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusGone:
		return true
	}
	return false
}

func generateError(resp *http.Response) error {
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return &HTTPError{StatusCode: resp.StatusCode, Body: "(could not read the response body)"}
	}
	return &HTTPError{StatusCode: resp.StatusCode, Body: string(respBytes)}
}

// getJSON runs a GET request and decodes a 200 response into v.
func getJSON(ctx context.Context, conf *config.EventLogConfig, db cache.Cache, url string, v any) error {
	resp, err := request(ctx, conf, db, url, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return generateError(resp)
	}
	// Keep numbers exact (json.Number) instead of float64, which loses
	// precision above 2^53.
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	return dec.Decode(v)
}

// RequestLogFiles lists EventLogFile records created in [since, until],
// ordered by CreatedDate, following every result page.
func RequestLogFiles(ctx context.Context, conf *config.EventLogConfig, db cache.Cache, since time.Time, until time.Time) ([]EventLogfileRecord, error) {
	soqlModel := MakeSoqlQuery("EventLogFile", "Id", "EventType", "CreatedDate", "LogDate", "LogFile", "Interval", "Sequence", "LogFileLength")
	if !conf.NoInterval {
		soqlModel.AndWhere("Interval = 'Hourly'")
	}
	soqlModel.AndWhere("CreatedDate >= " + since.UTC().Format(time.RFC3339))
	soqlModel.AndWhere("CreatedDate <= " + until.UTC().Format(time.RFC3339))
	if len(conf.EventTypes) > 0 {
		eventTypeFilter := make([]string, 0, len(conf.EventTypes))
		for _, eventType := range conf.EventTypes {
			eventTypeFilter = append(eventTypeFilter, "EventType = '"+eventType+"'")
		}
		soqlModel.AndOrWhere(eventTypeFilter...)
	}
	// Upstream assumed chronological order without asking for it.
	soqlModel.Tail("ORDER BY CreatedDate ASC, Id ASC")
	soql := soqlModel.Build()

	log.Debugf("Run EventLogFile SOQL query: %s", soql)

	url := queryURL(conf.Auth.TokenUrl+"/services/data/v"+conf.ApiVer+"/query", soql)
	var all []EventLogfileRecord
	for page := 0; url != ""; page++ {
		if page >= maxPages {
			return nil, fmt.Errorf("EventLogFile query exceeded %d pages", maxPages)
		}
		var response EventLogfileResponse
		if err := getJSON(ctx, conf, db, url, &response); err != nil {
			return nil, err
		}
		all = append(all, response.Records...)
		url = ""
		if !response.Done {
			if response.NextRecordsUrl == "" {
				return nil, fmt.Errorf("EventLogFile query returned an incomplete result set with no nextRecordsUrl (%d of %d records)", len(all), response.TotalSize)
			}
			url = conf.Auth.TokenUrl + response.NextRecordsUrl
		}
	}
	return all, nil
}

// DownloadCsvFile downloads a log file into dir and returns its path. The
// download is verified against the advertised length; a partial file is
// deleted and reported as an error.
func DownloadCsvFile(ctx context.Context, conf *config.EventLogConfig, db cache.Cache, record *EventLogfileRecord, dir string) (string, error) {
	// Large files may legitimately take longer than requestTimeout, so instead of
	// an overall deadline the download is cancelled when no bytes arrive for
	// the stall timeout (covering both the response headers and the body).
	stall := time.Duration(conf.DownloadStallTimeoutSeconds) * time.Second
	if stall <= 0 {
		stall = DefaultDownloadStallTimeout
	}
	dlCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	watchdog := time.AfterFunc(stall, func() {
		cancel(fmt.Errorf("no data received for %s", stall))
	})
	defer watchdog.Stop()

	resp, err := requestWithTimeout(dlCtx, conf, db, conf.Auth.TokenUrl+record.LogFile, 0, false)
	if err != nil {
		return "", withCause(dlCtx, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", generateError(resp)
	}

	if dir == "" {
		dir = os.TempDir()
	}
	outFile, err := os.CreateTemp(dir, "sf-elf-"+sanitizeFileName(record.Id)+"-*.csv")
	if err != nil {
		return "", err
	}
	filePath := outFile.Name()
	watchdog.Reset(stall)
	written, copyErr := io.Copy(outFile, &progressReader{r: resp.Body, onProgress: func() { watchdog.Reset(stall) }})
	copyErr = withCause(dlCtx, copyErr)
	closeErr := outFile.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr == nil && resp.ContentLength >= 0 && written != resp.ContentLength {
		copyErr = fmt.Errorf("downloaded %d bytes, expected Content-Length %d", written, resp.ContentLength)
	}
	if copyErr == nil && record.LogFileLength > 0 && float64(written) != record.LogFileLength {
		copyErr = fmt.Errorf("downloaded %d bytes, expected LogFileLength %.0f", written, record.LogFileLength)
	}
	if copyErr != nil {
		os.Remove(filePath)
		return "", fmt.Errorf("downloading log file %s: %w", record.Id, copyErr)
	}
	return filePath, nil
}

// queryURL percent-encodes a SOQL statement into the q parameter. Building the
// URL by hand corrupts any value containing +, &, # or %.
func queryURL(path, soql string) string {
	return path + "?" + url.Values{"q": {soql}}.Encode()
}

// DefaultDownloadStallTimeout cancels an EventLogFile download that receives no data for this long.
const DefaultDownloadStallTimeout = 120 * time.Second

type progressReader struct {
	r          io.Reader
	onProgress func()
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.onProgress()
	}
	return n, err
}

// withCause reports why a context was cancelled (e.g. the stall watchdog).
func withCause(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil && cause != ctx.Err() {
		return fmt.Errorf("%w: %v", err, cause)
	}
	return err
}

// RequestCustomQuery runs a custom SOQL query for [since, until], following every page.
func RequestCustomQuery(ctx context.Context, customQuery *config.QueryConfig, conf *config.EventLogConfig, db cache.Cache, since time.Time, until time.Time) ([]map[string]any, error) {
	soqlModel := MakeSoqlQuery(customQuery.Soql.From, customQuery.Soql.Select...)
	if customQuery.Soql.Where != "" {
		soqlModel.AndWhere(customQuery.Soql.Where)
	}
	if customQuery.EndTimestamp == "" {
		soqlModel.AndWhere(customQuery.Timestamp + " >= " + since.UTC().Format(time.RFC3339))
		soqlModel.AndWhere(customQuery.Timestamp + " <= " + until.UTC().Format(time.RFC3339))
	} else {
		// Select on the end field alone: a record is archived once it has
		// finished, in the window where it finished. Also bounding the start
		// field would skip records that started before "since" and finished
		// inside the window (e.g. a job running longer than the overlap).
		// Row de-duplication covers the overlap between windows.
		soqlModel.AndWhere(customQuery.EndTimestamp + " >= " + since.UTC().Format(time.RFC3339))
		soqlModel.AndWhere(customQuery.EndTimestamp + " <= " + until.UTC().Format(time.RFC3339))
	}
	soqlModel.Tail(customQuery.Soql.Tail)
	soql := soqlModel.Build()

	log.Debugf("Run custom SOQL query: %s", soql)

	path := conf.Auth.TokenUrl + "/services/data/v" + customQuery.ApiVer
	if customQuery.ApiName == "rest" {
		path += "/query"
	} else {
		path += "/tooling/query"
	}
	url := queryURL(path, soql)

	var all []map[string]any
	for page := 0; url != ""; page++ {
		if page >= maxPages {
			return nil, fmt.Errorf("custom query exceeded %d pages", maxPages)
		}
		var response GenericEventResponse
		if err := getJSON(ctx, conf, db, url, &response); err != nil {
			return nil, err
		}
		all = append(all, response.Records...)
		url = ""
		if !response.Done {
			if response.NextRecordsUrl == "" {
				// Truncated (e.g. a LIMIT in tail): advancing the watermark would
				// skip the rows that were never returned.
				return nil, fmt.Errorf("custom query on %s returned an incomplete result set with no nextRecordsUrl (%d of %d records); remove LIMIT/OFFSET from soql.tail", customQuery.Soql.From, len(all), response.TotalSize)
			}
			url = conf.Auth.TokenUrl + response.NextRecordsUrl
		}
	}
	return all, nil
}

// RequestLimits returns Salesforce org limits.
func RequestLimits(ctx context.Context, conf *config.EventLogConfig, db cache.Cache) (map[string]SingleLimitResponse, error) {
	limitsConf := &conf.Limits
	url := conf.Auth.TokenUrl + "/services/data/v" + limitsConf.ApiVer + "/limits"

	var response map[string]SingleLimitResponse
	if err := getJSON(ctx, conf, db, url, &response); err != nil {
		return nil, err
	}
	if len(limitsConf.Names) == 0 {
		return response, nil
	}
	filtered := map[string]SingleLimitResponse{}
	for _, limitName := range limitsConf.Names {
		if limit, ok := response[limitName]; ok {
			filtered[limitName] = limit
		}
	}
	return filtered, nil
}

var httpClient = &http.Client{}

// request performs an authenticated API GET using requestTimeout.
func request(ctx context.Context, conf *config.EventLogConfig, db cache.Cache, url string, isRetry bool) (*http.Response, error) {
	return requestWithTimeout(ctx, conf, db, url, time.Duration(conf.RequestTimeout)*time.Second, isRetry)
}

// requestWithTimeout performs an authenticated GET, re-authenticating once on 401.
// A zero timeout relies on ctx only.
func requestWithTimeout(ctx context.Context, conf *config.EventLogConfig, db cache.Cache, url string, timeout time.Duration, isRetry bool) (*http.Response, error) {
	accessToken, err := auth(conf, db)
	if err != nil {
		return nil, err
	}

	reqCtx := ctx
	var cancel context.CancelFunc = func() {}
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Add("User-Agent", getUserAgent())
	req.Header.Add("Authorization", "Bearer "+accessToken)

	// The upstream code mutated http.DefaultClient.Timeout on every request.
	resp, err := httpClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized && !isRetry {
		resp.Body.Close()
		cancel()
		log.Warnf("Wrong credentials error (401). Try relogging...")
		if err := relogin(conf, db); err != nil {
			return nil, err
		}
		return requestWithTimeout(ctx, conf, db, url, timeout, true)
	}
	resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func getUserAgent() string {
	return fmt.Sprintf("%s/%s (%s; %s)", archive.CollectorName, archive.CollectorVersion, runtime.GOOS, runtime.GOARCH)
}

func sanitizeFileName(s string) string {
	return filepath.Base(filepath.Clean("/" + s))
}
