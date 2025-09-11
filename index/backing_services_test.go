package index_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/tern/v2/migrate"
	"github.com/ttab/elephant-index/schema"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/ttab/elsinod"
	"github.com/ttab/eltest"
	"github.com/ttab/howdah"
)

type Environment struct {
	PostgresURI   string
	OpenSearchURI string
	Migrator      *migrate.Migrator
	OIDCConfig    string
	Repository    *Repository
}

type T interface {
	test.TestingT

	Name() string
	Context() context.Context
	Cleanup(fn func())
}

func SetUpBackingServices(
	t T,
	instrument *elephantine.HTTPClientInstrumentation,
	skipMigrations bool,
) Environment {
	t.Helper()

	ctx := t.Context()

	minio := eltest.NewMinio(t, eltest.Minio202509)
	pg := eltest.NewPostgres(t, eltest.Postgres17_6)
	opensearch := eltest.NewOpenSearch(t, eltest.OpenSearch2_19)

	oidcURL := runElsinod(t)

	var client http.Client

	err := instrument.Client("s3", &client)
	test.Must(t, err, "instrument s3 http client")

	s3Client, err := getS3Client(ctx, &client, minio.Environment())
	test.Must(t, err, "get S3 client")

	bucket := strings.ToLower(t.Name() + "-repo")

	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	test.Must(t, err, "create repo bucket")

	assetBucket := bucket + "-assets"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(assetBucket),
	})
	test.Must(t, err, "create repo asset bucket")

	_, err = s3Client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(assetBucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	test.Must(t, err, "enable asset bucket versioning")

	repoPGEnv := pg.Database(t, "repo", nil, false)

	minioEnv := minio.Environment()

	repo := NewRepository(t, RepositoryConfig{
		ConnStr:       repoPGEnv.PostgresURI,
		S3Endpoint:    "http://" + minioEnv.Endpoint,
		ArchiveBucket: bucket,
		AssetBucket:   assetBucket,
		OIDCConfig:    oidcURL,
	})

	indexPGEnv := pg.Database(t, "index", schema.Migrations, true)

	env := Environment{
		PostgresURI:   indexPGEnv.PostgresURI,
		OpenSearchURI: opensearch.GetEndpoint(),
		OIDCConfig:    oidcURL,
		Repository:    repo,
	}

	conn, err := pgx.Connect(ctx, env.PostgresURI)
	test.Must(t, err, "open postgres user connection")

	defer conn.Close(ctx)

	m, err := migrate.NewMigrator(ctx, conn, "schema_vesion")
	test.Must(t, err, "create migrator")

	err = m.LoadMigrations(schema.Migrations)
	test.Must(t, err, "create load migrations")

	if !skipMigrations {
		err = m.Migrate(ctx)
		test.Must(t, err, "migrate to current DB schema")
	}

	env.Migrator = m

	return env
}

// Start an elsinod server without the UI.
//
// TODO: This could probably be extracted to a shared helper in the future. But
// holding off until we are writing integration tests for something else that
// needs it.
func runElsinod(t T) string {
	t.Helper()

	ctx := t.Context()

	dockerIP, err := eltest.GetGatewayIP()
	test.Must(t, err, "get Docker gateway IP")

	// Listen on random available port.
	elsinodListener, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP: dockerIP,
	})
	test.Must(t, err, "create elsinod listener")

	var serverCleansUp atomic.Bool

	t.Cleanup(func() {
		if serverCleansUp.Load() {
			return
		}

		err := elsinodListener.Close()
		test.Must(t, err, "close elsinod listener")
	})

	elsinodPubURL := "http://" + elsinodListener.Addr().String()

	signingKey, err := elsinod.NewSigningKey()
	test.Must(t, err, "create signing key")

	keyStore := elsinod.NewStaticKeyStore("k1", signingKey)

	els, err := elsinod.New(ctx, keyStore, elsinodPubURL, "pass", "pass", "example")
	test.Must(t, err, "create elsinod")

	elsinodMux := http.NewServeMux()
	pageMux := howdah.NewPageMux(nil, elsinodMux)

	els.RegisterRoutes(pageMux)

	elsServer := http.Server{
		Handler:           elsinodMux,
		ReadHeaderTimeout: 1 * time.Second,
	}

	go func() {
		serverCleansUp.Store(true)

		t.Cleanup(func() {
			err := elsServer.Close()
			test.Must(t, err, "close elsinod server")
		})

		err := elsServer.Serve(elsinodListener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = elsinodListener.Close()

			t.Fatalf("start elsinod server: %v", err)
		}
	}()

	return elsinodPubURL + "/.well-known/openid-configuration"
}

func getS3Client(
	ctx context.Context, client *http.Client, env eltest.MinioEnvironment,
) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithHTTPClient(client),
		config.WithDefaultRegion("eu-north-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				env.ID, env.Secret, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("create S3 config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + env.Endpoint)
		o.UsePathStyle = true
		o.EndpointOptions.DisableHTTPS = true
	})

	return s3Client, nil
}
