// sf-archive-verify reconciles an S3 archive.
//
// It checks every manifest against its data object (record count, size and
// SHA-256) and, when a mock Salesforce ledger URL is given, reports events the
// mock generated that are missing from the archive. Exit code 0 means no
// missing records and no manifest mismatches.
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
	ObjectsWithoutMan  int            `json:"objectsWithoutManifest"`
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
	ledgerURL := flag.String("ledger", "", "mock Salesforce ledger URL, e.g. http://localhost:8080/admin/ledger")
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
			fail(err)
		}
		manifests[m.ObjectKey] = m
	}

	// Identifiers seen per stream topic / event log / custom object.
	var mu sync.Mutex
	streamIds := map[string]map[string]int{}
	elfIds := map[string]int{}
	customIds := map[string]map[string]int{}
	add := func(m map[string]map[string]int, group, id string) {
		if m[group] == nil {
			m[group] = map[string]int{}
		}
		m[group][id]++
	}

	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for _, key := range dataKeys {
		wg.Add(1)
		sem <- struct{}{}
		go func(key string) {
			defer wg.Done()
			defer func() { <-sem }()
			obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(*bucket), Key: aws.String(key)})
			if err != nil {
				fail(err)
			}
			body, err := io.ReadAll(obj.Body)
			obj.Body.Close()
			if err != nil {
				fail(err)
			}
			sum := sha256.Sum256(body)
			var count int64
			local := []archive.Line{}
			if err := archive.DecodeObject(strings.NewReader(string(body)), func(l archive.Line) error {
				count++
				local = append(local, l)
				return nil
			}); err != nil {
				fail(fmt.Errorf("%s: %w", key, err))
			}

			mu.Lock()
			defer mu.Unlock()
			m, ok := manifests[key]
			if !ok {
				rep.ObjectsWithoutMan++
			} else if m.RecordCount != count || m.SHA256 != hex.EncodeToString(sum[:]) || m.CompressedBytes != int64(len(body)) {
				rep.ManifestMismatches = append(rep.ManifestMismatches, key)
			}
			for _, l := range local {
				rep.RecordsBySource[l.Source]++
				switch l.Source {
				case archive.SourceStream:
					topic := ""
					if ok {
						topic = m.Lineage["topic"]
					}
					id, _ := l.Attributes["EventIdentifier"].(string)
					add(streamIds, topic, id)
				case archive.SourceEventLog:
					id, _ := l.Attributes["REQUEST_ID"].(string)
					elfIds[id]++
				case archive.SourceSOQL:
					id, _ := l.Attributes["Id"].(string)
					add(customIds, l.EventType, id)
				}
			}
		}(key)
	}
	wg.Wait()

	rep.OK = len(rep.ManifestMismatches) == 0 && rep.ObjectsWithoutMan == 0
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
