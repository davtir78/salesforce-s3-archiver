package grpcclient

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream/pubsub/common"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream/pubsub/proto"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/davtir78/salesforce-s3-archiver/internal/oauth"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	tokenHeader      = "accesstoken"
	instanceHeader   = "instanceurl"
	tenantHeader     = "tenantid"
	userInfoEndpoint = "/services/oauth2/userinfo"
)

type UserInfoResponse struct {
	UserID         string `json:"user_id"`
	OrganizationID string `json:"organization_id"`
}

// PubSubClient wraps a Pub/Sub API connection and its OAuth session.
type PubSubClient struct {
	auth config.AuthConfig

	mu          sync.RWMutex
	accessToken string
	instanceURL string
	userID      string
	orgID       string

	conn         *grpc.ClientConn
	pubSubClient proto.PubSubClient

	schemaMu    sync.Mutex
	schemaCache map[string]*schemaEntry
}

type Options struct {
	Endpoint string
	Insecure bool
	Auth     config.AuthConfig
}

// NewGRPCClient creates a client for the configured Pub/Sub endpoint.
func NewGRPCClient(opts Options) (*PubSubClient, error) {
	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint = common.GRPCEndpoint
	}
	var creds credentials.TransportCredentials
	if opts.Insecure {
		log.Warnf("Pub/Sub connection to %s is not encrypted (pubsubInsecure: true)", endpoint)
		creds = insecure.NewCredentials()
	} else {
		creds = credentials.NewClientTLSFromCert(getCerts(), "")
	}
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64*1024*1024)),
	)
	if err != nil {
		return nil, err
	}
	return &PubSubClient{
		auth:         opts.Auth,
		conn:         conn,
		pubSubClient: proto.NewPubSubClient(conn),
		schemaCache:  make(map[string]*schemaEntry),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *PubSubClient) Close() {
	c.conn.Close()
}

// Authenticate obtains a new access token and the org/user identity.
func (c *PubSubClient) Authenticate(ctx context.Context) error {
	resp, err := oauth.Login(c.auth)
	if err != nil {
		return fmt.Errorf("login failed: %w", err)
	}
	info, err := UserInfo(ctx, c.auth.TokenUrl, resp.AccessToken)
	if err != nil {
		return fmt.Errorf("fetching user info: %w", err)
	}
	if info.OrganizationID == "" {
		return fmt.Errorf("user info response has no organization_id")
	}
	c.mu.Lock()
	c.accessToken = resp.AccessToken
	c.instanceURL = resp.InstanceURL
	c.userID = info.UserID
	c.orgID = info.OrganizationID
	c.mu.Unlock()
	log.Debugf("Authenticated to Salesforce org %s", info.OrganizationID)
	return nil
}

func (c *PubSubClient) OrgId() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.orgID
}

func UserInfo(ctx context.Context, tokenEndpoint string, accessToken string) (*UserInfoResponse, error) {
	ctx, cancelFn := context.WithTimeout(ctx, common.GRPCCallTimeout)
	defer cancelFn()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenEndpoint+userInfoEndpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	httpResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200 status code returned on OAuth user info call: %v", httpResp.StatusCode)
	}

	var userInfoResponse UserInfoResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&userInfoResponse); err != nil {
		return nil, err
	}
	return &userInfoResponse, nil
}

// AuthContext returns ctx carrying the Pub/Sub authentication headers.
func (c *PubSubClient) AuthContext(ctx context.Context) context.Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(
		tokenHeader, c.accessToken,
		instanceHeader, c.instanceURL,
		tenantHeader, c.orgID,
	))
}

// Subscribe opens a bidirectional subscription stream. The caller sends the
// initial FetchRequest.
func (c *PubSubClient) Subscribe(ctx context.Context) (proto.PubSub_SubscribeClient, error) {
	return c.pubSubClient.Subscribe(c.AuthContext(ctx))
}

func (c *PubSubClient) GetSchema(ctx context.Context, schemaId string) (*proto.SchemaInfo, error) {
	ctx, cancelFn := context.WithTimeout(c.AuthContext(ctx), common.GRPCCallTimeout)
	defer cancelFn()
	var trailer metadata.MD
	resp, err := c.pubSubClient.GetSchema(ctx, &proto.SchemaRequest{SchemaId: schemaId}, grpc.Trailer(&trailer))
	printTrailer(trailer)
	return resp, err
}

// DecodeEvent decodes an event payload with its Avro schema. It returns the
// flattened field map and the event type name (e.g. "LoginEventStream").
func (c *PubSubClient) DecodeEvent(ctx context.Context, event *proto.ConsumerEvent) (map[string]any, string, error) {
	entry, err := c.fetchSchema(ctx, event.GetEvent().GetSchemaId())
	if err != nil {
		return nil, "", fmt.Errorf("fetching schema %s: %w", event.GetEvent().GetSchemaId(), err)
	}
	parsed, _, err := entry.codec.NativeFromBinary(event.GetEvent().GetPayload())
	if err != nil {
		return nil, "", fmt.Errorf("decoding avro payload: %w", err)
	}
	body, ok := parsed.(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("decoded payload is %T, not a record", parsed)
	}
	fields, err := entry.flattener.Flatten(body)
	if err != nil {
		return nil, "", fmt.Errorf("flattening avro record: %w", err)
	}
	return fields, shortTypeName(parseTypeName(entry.codec)), nil
}

type schemaEntry struct {
	codec     *goavro.Codec
	flattener *Flattener
}

func (c *PubSubClient) fetchSchema(ctx context.Context, schemaId string) (*schemaEntry, error) {
	c.schemaMu.Lock()
	defer c.schemaMu.Unlock()
	if entry, ok := c.schemaCache[schemaId]; ok {
		return entry, nil
	}
	log.Debugf("Making GetSchema request for uncached schema %s", schemaId)
	schema, err := c.GetSchema(ctx, schemaId)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema.GetSchemaJson())
	if err != nil {
		return nil, err
	}
	flattener, err := NewFlattener(schema.GetSchemaJson())
	if err != nil {
		return nil, err
	}
	entry := &schemaEntry{codec: codec, flattener: flattener}
	c.schemaCache[schemaId] = entry
	return entry, nil
}

func parseTypeName(codec *goavro.Codec) string {
	var dat map[string]any
	if err := json.Unmarshal([]byte(codec.CanonicalSchema()), &dat); err != nil {
		return ""
	}
	name, _ := dat["name"].(string)
	return name
}

func shortTypeName(fullName string) string {
	parts := strings.Split(fullName, ".")
	if name := parts[len(parts)-1]; name != "" {
		return name
	}
	return "UnknownEvent"
}

// getCerts returns the system cert pool, or an empty pool if unavailable.
func getCerts() *x509.CertPool {
	if certs, err := x509.SystemCertPool(); err == nil {
		return certs
	}
	return x509.NewCertPool()
}

func printTrailer(trailer metadata.MD) {
	if !log.IsDebugEnabled() || len(trailer) == 0 {
		return
	}
	for key, val := range trailer {
		log.Debugf("[trailer] %s = %s", key, val)
	}
}
