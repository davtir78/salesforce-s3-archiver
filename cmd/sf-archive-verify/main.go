// sf-archive-verify reconciles an S3 archive.
//
// It streams every data object, checking it against its manifest (record
// count, compressed and uncompressed size, SHA-256) and reporting manifests
// whose data object is missing. With -ledger it also reconciles the archived
// records against the mock Salesforce ledger.
//
// Exit code 0 means nothing is missing and every manifest matches.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/mocksf"
)

type report struct {
	Objects            int            `json:"objects"`
	Manifests          int            `json:"manifests"`
	ManifestMismatches []string       `json:"manifestMismatches"`
	ManifestsNoObject  []string       `json:"manifestsWithoutObject"`
	ObjectsWithoutMan  int            `json:"objectsWithoutManifest"`
	UnreadableObjects  []string       `json:"unreadableObjects"`
	RecordsBySource    map[string]int `json:"recordsBySource"`
	Streams            map[string]gap `json:"streams,omitempty"`
	EventLogRows       *gap           `json:"eventLogRows,omitempty"`
	CustomRecords      map[string]gap `json:"customRecords,omitempty"`
	OK                 bool           `json:"ok"`
}

type gap struct {
	Expected   int      `json:"expected"`
	Archived   int      `json:"archivedUnique"`
	Missing    int      `json:"missing"`
	Duplicates int      `json:"duplicates"`
	Examples   []string `json:"missingExamples,omitempty"`
}

func main() {
	bucket := flag.String("bucket", "", "S3 bucket")
	prefix := flag.String("prefix", "", "archive prefix")
	endpoint := flag.String("endpoint", "", "S3 endpoint override (e.g. MinIO)")
	region := flag.String("region", "", "AWS region")
	strict := flag.Bool("strict", false, "also fail on data objects without a manifest (normally duplicates left by a failed manifest upload that the collector re-archived)")
	ledgerURL := flag.String("ledger", "", "mock Salesforce ledger URL, e.g. http://localhost:8080/admin/ledger")
	workers := flag.Int("workers", 8, "objects read concurrently")
	out := flag.String("out", "", "write the JSON report to this file")
	flag.Parse()
	if *bucket == "" {
		fmt.Fprintln(os.Stderr, "-bucket is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	sink, err := archive.NewS3Sink(ctx, archive.S3Options{Bucket: *bucket, Prefix: *prefix, Endpoint: *endpoint, ForcePathStyle: *endpoint != "", Region: *region})
	if err != nil {
		fail(err)
	}
	client := sink.Client()
	base := strings.Trim(*prefix, "/")
	if base != "" {
		base += "/"
	}

	dataKeys, err := list(ctx, client, *bucket, base+"raw/")
	if err != nil {
		fail(err)
	}
	manifestKeys, err := list(ctx, client, *bucket, base+"manifests/")
	if err != nil {
		fail(err)
	}

	rep := report{Objects: len(dataKeys), Manifests: len(manifestKeys), RecordsBySource: map[string]int{}}
	manifests := map[string]archive.Manifest{}
	seenManifest := map[string]bool{}
	for _, k := range manifestKeys {
		var m archive.Manifest
		if err := getJSON(ctx, client, *bucket, k, &m); err != nil {
			fail(fmt.Errorf("reading manifest %s: %w", k, err))
		}
		if m.ObjectKey == "" {
			rep.ManifestMismatches = append(rep.ManifestMismatches, k+" (manifest has no objectKey)")
			continue
		}
		if _, dup := manifests[m.ObjectKey]; dup {
			rep.ManifestMismatches = append(rep.ManifestMismatches, k+" (duplicate objectKey "+m.ObjectKey+")")
			continue
		}
		manifests[m.ObjectKey] = m
	}

	var (
		mu        sync.Mutex
		streamIds = map[string]map[string]int{}
		elfIds    = map[string]int{}
		customIds = map[string]map[string]int{}
	)
	add := func(m map[string]map[string]int, group, id string) {
		if m[group] == nil {
			m[group] = map[string]int{}
		}
		m[group][id]++
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, *workers)
	for _, key := range dataKeys {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			stats, lines, err := readObject(ctx, client, *bucket, key)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				// One unreadable object must not abandon the whole report.
				rep.UnreadableObjects = append(rep.UnreadableObjects, fmt.Sprintf("%s: %v", key, err))
				return
			}
			m, ok := manifests[key]
			if !ok {
				rep.ObjectsWithoutMan++
			} else {
				seenManifest[key] = true
				if why := stats.compare(m); why != "" {
					rep.ManifestMismatches = append(rep.ManifestMismatches, key+" ("+why+")")
				}
			}
			for _, l := range lines {
				rep.RecordsBySource[l.Source]++
				switch l.Source {
				case archive.SourceStream:
					topic := ""
					if ok {
						topic = m.Lineage["topic"]
					}
					// The mock ledger tracks EventIdentifier; event_id is EventUuid.
					id, _ := l.Payload["EventIdentifier"].(string)
					add(streamIds, topic, id)
				case archive.SourceEventLog:
					id, _ := l.Payload["REQUEST_ID"].(string)
					elfIds[id]++
				case archive.SourceSOQL:
					id, _ := l.Payload["Id"].(string)
					add(customIds, l.EventType, id)
				}
			}
		}()
	}
	wg.Wait()

	for key := range manifests {
		if !seenManifest[key] {
			rep.ManifestsNoObject = append(rep.ManifestsNoObject, key)
		}
	}
	sort.Strings(rep.ManifestsNoObject)
	sort.Strings(rep.ManifestMismatches)

	rep.OK = len(rep.ManifestMismatches) == 0 &&
		len(rep.ManifestsNoObject) == 0 &&
		len(rep.UnreadableObjects) == 0 &&
		(!*strict || rep.ObjectsWithoutMan == 0)

	if *ledgerURL != "" {
		var ledger mocksf.Ledger
		if err := fetchJSON(*ledgerURL, &ledger); err != nil {
			fail(err)
		}
		rep.Streams = map[string]gap{}
		for topic, ids := range ledger.Streams {
			g := compare(ids, streamIds[topic])
			rep.Streams[topic] = g
			rep.OK = rep.OK && g.Missing == 0
		}
		var allRows []string
		for _, ids := range ledger.EventLogFiles {
			allRows = append(allRows, ids...)
		}
		g := compare(allRows, elfIds)
		rep.EventLogRows = &g
		rep.OK = rep.OK && g.Missing == 0
		rep.CustomRecords = map[string]gap{}
		for obj, ids := range ledger.CustomRecords {
			g := compare(ids, customIds[obj])
			rep.CustomRecords[obj] = g
			rep.OK = rep.OK && g.Missing == 0
		}
	}

	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		os.WriteFile(*out, b, 0o644)
	}
	if !rep.OK {
		os.Exit(1)
	}
}

// objectStats is what a data object actually contains.
type objectStats struct {
	records      int64
	compressed   int64
	uncompressed int64
	sha256       string
	first, last  *time.Time
}

func (s objectStats) compare(m archive.Manifest) string {
	var problems []string
	if s.records != m.RecordCount {
		problems = append(problems, fmt.Sprintf("record count %d != manifest %d", s.records, m.RecordCount))
	}
	if s.compressed != m.CompressedBytes {
		problems = append(problems, fmt.Sprintf("compressed bytes %d != manifest %d", s.compressed, m.CompressedBytes))
	}
	if s.uncompressed != m.UncompressedBytes {
		problems = append(problems, fmt.Sprintf("uncompressed bytes %d != manifest %d", s.uncompressed, m.UncompressedBytes))
	}
	if s.sha256 != m.SHA256 {
		problems = append(problems, "sha256 mismatch")
	}
	if !sameTime(s.first, m.FirstTimestamp) || !sameTime(s.last, m.LastTimestamp) {
		problems = append(problems, "first/last timestamp mismatch")
	}
	return strings.Join(problems, "; ")
}

func sameTime(a, b *time.Time) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Equal(*b)
	}
}

// readObject streams the object once: hashing and counting the compressed bytes
// while decoding the NDJSON, instead of holding several copies in memory.
func readObject(ctx context.Context, client *s3.Client, bucket, key string) (objectStats, []archive.Line, error) {
	obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return objectStats{}, nil, err
	}
	defer obj.Body.Close()

	hasher := sha256.New()
	counter := &countingReader{r: io.TeeReader(obj.Body, hasher)}
	var (
		stats objectStats
		lines []archive.Line
	)
	uncompressed, err := archive.DecodeObjectCounted(counter, func(l archive.Line) error {
		stats.records++
		ts := l.Timestamp
		if stats.first == nil || ts.Before(*stats.first) {
			t := ts
			stats.first = &t
		}
		if stats.last == nil || ts.After(*stats.last) {
			t := ts
			stats.last = &t
		}
		lines = append(lines, l)
		return nil
	})
	if err != nil {
		return objectStats{}, nil, err
	}
	// Drain any trailer the gzip reader left so the byte count and hash cover
	// the whole object.
	if _, err := io.Copy(io.Discard, counter); err != nil {
		return objectStats{}, nil, err
	}
	stats.compressed = counter.n
	stats.uncompressed = uncompressed
	stats.sha256 = hex.EncodeToString(hasher.Sum(nil))
	return stats, lines, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func compare(expected []string, seen map[string]int) gap {
	g := gap{Expected: len(expected), Archived: len(seen)}
	for _, id := range expected {
		if seen[id] == 0 {
			g.Missing++
			if len(g.Examples) < 10 {
				g.Examples = append(g.Examples, id)
			}
		}
	}
	for _, c := range seen {
		if c > 1 {
			g.Duplicates += c - 1
		}
	}
	sort.Strings(g.Examples)
	return g
}

func list(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys, nil
}

func getJSON(ctx context.Context, client *s3.Client, bucket, key string, v any) error {
	obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return err
	}
	defer obj.Body.Close()
	return json.NewDecoder(obj.Body).Decode(v)
}

func fetchJSON(url string, v any) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(2)
}
