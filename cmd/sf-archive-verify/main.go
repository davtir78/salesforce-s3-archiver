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
	Objects            int      `json:"objects"`
	Manifests          int      `json:"manifests"`
	ManifestMismatches []string `json:"manifestMismatches"`
	ManifestsNoObject  []string `json:"manifestsWithoutObject"`
	ObjectsWithoutMan  int      `json:"objectsWithoutManifest"`
	UnreadableObjects  []string `json:"unreadableObjects"`
	// Manifests that could not be read or parsed.
	UnreadableManifests []string `json:"unreadableManifests"`
	// Manifests in a format this verifier does not read (e.g. written before
	// the record envelope); their objects are not decoded.
	UnsupportedManifests []string       `json:"unsupportedManifests"`
	RecordsBySource      map[string]int `json:"recordsBySource"`
	Streams              map[string]gap `json:"streams,omitempty"`
	EventLogRows         *gap           `json:"eventLogRows,omitempty"`
	CustomRecords        map[string]gap `json:"customRecords,omitempty"`
	OK                   bool           `json:"ok"`
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
	for _, k := range manifestKeys {
		var m archive.Manifest
		if err := getJSON(ctx, client, *bucket, k, &m); err != nil {
			// Report it and keep going; one corrupt manifest must not hide the rest.
			rep.UnreadableManifests = append(rep.UnreadableManifests, fmt.Sprintf("%s: %v", k, err))
			continue
		}
		if m.ObjectKey == "" {
			rep.ManifestMismatches = append(rep.ManifestMismatches, k+" (manifest has no objectKey)")
			continue
		}
		if _, dup := manifests[m.ObjectKey]; dup {
			rep.ManifestMismatches = append(rep.ManifestMismatches, k+" (duplicate objectKey "+m.ObjectKey+")")
			continue
		}
		if m.ManifestVersion != archive.ManifestVersion {
			// Earlier line formats do not decode as envelopes; comparing them
			// would report every record missing rather than the real cause.
			rep.UnsupportedManifests = append(rep.UnsupportedManifests,
				fmt.Sprintf("%s (manifestVersion %d, this verifier reads %d)", k, m.ManifestVersion, archive.ManifestVersion))
		}
		manifests[m.ObjectKey] = m
	}

	// Record identifiers are only kept when reconciling against a ledger, and
	// then only the identifier, not the record.
	reconcile := *ledgerURL != ""
	var (
		mu           sync.Mutex
		seenManifest = map[string]bool{}
		streamIds    = map[string]map[string]int{}
		elfIds       = map[string]int{}
		customIds    = map[string]map[string]int{}
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
		m, hasManifest := manifests[key] // manifests is read-only from here on
		if hasManifest && m.ManifestVersion != archive.ManifestVersion {
			mu.Lock()
			seenManifest[key] = true // already reported as unsupported
			mu.Unlock()
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			bySource := map[string]int{}
			var refs []recordRef
			stats, err := readObject(ctx, client, *bucket, key, func(l archive.Line) {
				bySource[l.Source]++
				if reconcile {
					refs = append(refs, refFor(l, m.Lineage["topic"]))
				}
			})

			mu.Lock()
			defer mu.Unlock()
			if hasManifest {
				// Reported once: as unreadable or mismatched, never also as a
				// manifest without an object.
				seenManifest[key] = true
			}
			if err != nil {
				// One unreadable object must not abandon the whole report.
				rep.UnreadableObjects = append(rep.UnreadableObjects, fmt.Sprintf("%s: %v", key, err))
				return
			}
			if !hasManifest {
				rep.ObjectsWithoutMan++
			} else if why := stats.compare(m); why != "" {
				rep.ManifestMismatches = append(rep.ManifestMismatches, key+" ("+why+")")
			}
			for source, n := range bySource {
				rep.RecordsBySource[source] += n
			}
			for _, r := range refs {
				switch r.source {
				case archive.SourceStream:
					add(streamIds, r.group, r.id)
				case archive.SourceEventLog:
					elfIds[r.id]++
				case archive.SourceSOQL:
					add(customIds, r.group, r.id)
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
	for _, list := range [][]string{rep.ManifestsNoObject, rep.ManifestMismatches, rep.UnreadableObjects, rep.UnreadableManifests, rep.UnsupportedManifests} {
		sort.Strings(list)
	}

	rep.OK = len(rep.ManifestMismatches) == 0 &&
		len(rep.ManifestsNoObject) == 0 &&
		len(rep.UnreadableObjects) == 0 &&
		len(rep.UnreadableManifests) == 0 &&
		len(rep.UnsupportedManifests) == 0 &&
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

// readObject streams a data object from S3 through scanObject.
func readObject(ctx context.Context, client *s3.Client, bucket, key string, visit func(archive.Line)) (objectStats, error) {
	obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return objectStats{}, err
	}
	defer obj.Body.Close()
	return scanObject(obj.Body, visit)
}

// scanObject reads the object once, hashing and counting the compressed bytes
// while decoding the NDJSON. Each record is passed to visit and then dropped, so
// memory does not grow with the object.
func scanObject(r io.Reader, visit func(archive.Line)) (objectStats, error) {
	hasher := sha256.New()
	counter := &countingReader{r: io.TeeReader(r, hasher)}
	var (
		stats objectStats
		times archive.TimestampRange
	)
	uncompressed, err := archive.DecodeObjectCounted(counter, func(l archive.Line) error {
		stats.records++
		times.Add(l.Timestamp)
		if visit != nil {
			visit(l)
		}
		return nil
	})
	if err != nil {
		return objectStats{}, err
	}
	// Drain any trailer the gzip reader left so the byte count and hash cover
	// the whole object.
	if _, err := io.Copy(io.Discard, counter); err != nil {
		return objectStats{}, err
	}
	stats.first, stats.last = times.First, times.Last
	stats.compressed = counter.n
	stats.uncompressed = uncompressed
	stats.sha256 = hex.EncodeToString(hasher.Sum(nil))
	return stats, nil
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

// recordRef is the part of a record the ledger reconciliation needs.
type recordRef struct {
	source, group, id string
}

// refFor extracts the identifier the mock ledger tracks for each source.
func refFor(l archive.Line, topic string) recordRef {
	switch l.Source {
	case archive.SourceStream:
		// The mock ledger tracks EventIdentifier; event_id is EventUuid.
		id, _ := l.Payload["EventIdentifier"].(string)
		return recordRef{l.Source, topic, id}
	case archive.SourceEventLog:
		id, _ := l.Payload["REQUEST_ID"].(string)
		return recordRef{l.Source, "", id}
	case archive.SourceSOQL:
		id, _ := l.Payload["Id"].(string)
		return recordRef{l.Source, l.EventType, id}
	}
	return recordRef{source: l.Source}
}
