package archive

// syncDir is a no-op on Windows, where directories cannot be opened for sync.
// The local sink is a development aid; S3 is the durable destination.
func syncDir(dir string) error { return nil }
