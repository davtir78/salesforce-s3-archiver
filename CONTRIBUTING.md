# Contributing

Issues and pull requests are welcome.

- Run `gofmt`, `go vet ./...` and `go test ./...` before opening a pull request.
- Changes to the stream subscriber, checkpoint store or event log watermarks must keep the
  invariant that no checkpoint or watermark moves past data that is not durably archived.
  Extend the crash-matrix or chaos tests to cover new failure modes.
- Never include Salesforce credentials, access tokens, org data or AWS account details in
  issues, logs or tests.

By contributing you agree that your contributions are licensed under the Apache License 2.0.
