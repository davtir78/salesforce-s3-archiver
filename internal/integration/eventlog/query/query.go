package query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

func generateError(resp *http.Response) error {
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return fmt.Errorf("Error %d (could not read the response body)", resp.StatusCode)
	}
	return fmt.Errorf("Error %d, body: %s", resp.StatusCode, respBytes)
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
	return json.NewDecoder(resp.Body).Decode(v)
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

	url := conf.Auth.TokenUrl + "/services/data/v" + conf.ApiVer + "/query?q=" + soql
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
		if !response.Done && response.NextRecordsUrl != "" {
			url = conf.Auth.TokenUrl + response.NextRecordsUrl
		}
	}
	return all, nil
}

// DownloadCsvFile downloads a log file into dir and returns its path. The
// download is verified against the advertised length; a partial file is
// deleted and reported as an error.
func DownloadCsvFile(ctx context.Context, conf *config.EventLogConfig, db cache.Cache, record *EventLogfileRecord, dir string) (string, error) {
	// Large files: no overall timeout beyond ctx (requestTimeout applies to API calls).
	resp, err := requestWithTimeout(ctx, conf, db, conf.Auth.TokenUrl+record.LogFile, 0, false)
	if err != nil {
		return "", err
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
	written, copyErr := io.Copy(outFile, resp.Body)
	closeErr := outFile.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr == nil && resp.ContentLength >= 0 && written != resp.ContentLength {
		copyErr = fmt.Errorf("downloaded %d bytes, expected Content-Length %d", written, resp.ContentLength)
	}
	if copyErr == nil && record.LogFileLength > 0 && written != record.LogFileLength {
		copyErr = fmt.Errorf("downloaded %d bytes, expected LogFileLength %d", written, record.LogFileLength)
	}
	if copyErr != nil {
		os.Remove(filePath)
		return "", fmt.Errorf("downloading log file %s: %w", record.Id, copyErr)
	}
	return filePath, nil
}

// RequestCustomQuery runs a custom SOQL query for [since, until], following every page.
func RequestCustomQuery(ctx context.Context, customQuery *config.QueryConfig, conf *config.EventLogConfig, db cache.Cache, since time.Time, until time.Time) ([]map[string]any, error) {
	soqlModel := MakeSoqlQuery(customQuery.Soql.From, customQuery.Soql.Select...)
	if customQuery.Soql.Where != "" {
		soqlModel.AndWhere(customQuery.Soql.Where)
	}
	soqlModel.AndWhere(customQuery.Timestamp + " >= " + since.UTC().Format(time.RFC3339))
	if customQuery.EndTimestamp == "" {
		soqlModel.AndWhere(customQuery.Timestamp + " <= " + until.UTC().Format(time.RFC3339))
	} else {
		soqlModel.AndWhere(customQuery.EndTimestamp + " <= " + until.UTC().Format(time.RFC3339))
	}
	soqlModel.Tail(customQuery.Soql.Tail)
	soql := soqlModel.Build()

	log.Debugf("Run custom SOQL query: %s", soql)

	url := conf.Auth.TokenUrl + "/services/data/v" + customQuery.ApiVer
	if customQuery.ApiName == "rest" {
		url += "/query"
	} else {
		url += "/tooling/query"
	}
	url += "?q=" + soql

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
		if !response.Done && response.NextRecordsUrl != "" {
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
