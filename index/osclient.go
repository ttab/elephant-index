package index

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/signer/awsv2"
	"github.com/ttab/elephant-index/postgres"
)

type ClusterGetter interface {
	GetCluster(ctx context.Context, name string) (postgres.Cluster, error)
}

type ClusterAuth struct {
	IAM      bool
	Username string
	Password string
}

func (ca *ClusterAuth) SetPassword(password string, passwordKey [32]byte) error {
	enc, err := encryptPassword(password, passwordKey)
	if err != nil {
		return fmt.Errorf("encrypt password: %w", err)
	}

	ca.Password = enc

	return nil
}

func (ca *ClusterAuth) GetPassword(passwordKey [32]byte) (string, error) {
	pass, _, err := decryptPassword(ca.Password, passwordKey)
	if err != nil {
		return "", fmt.Errorf("decrypt password: %w", err)
	}

	return pass, nil
}

type OSClientProvider struct {
	clusters    ClusterGetter
	passwordKey [32]byte
}

func NewOSClientProvider(
	clusters ClusterGetter,
	passwordKey [32]byte,
) *OSClientProvider {
	return &OSClientProvider{
		clusters:    clusters,
		passwordKey: passwordKey,
	}
}

func (o *OSClientProvider) GetClientForCluster(
	ctx context.Context, cluster string,
) (*opensearch.Client, error) {
	c, err := o.clusters.GetCluster(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("load cluster details: %w", err)
	}

	var auth ClusterAuth

	err = json.Unmarshal(c.Auth, &auth)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster authentication details: %w", err)
	}

	var username, password string

	if auth.Username != "" {
		pw, err := auth.GetPassword(o.passwordKey)
		if err != nil {
			return nil, fmt.Errorf("get cluster password: %w", err)
		}

		username, password = auth.Username, pw
	}

	osConfig := opensearch.Config{
		Addresses: []string{c.Url},
	}

	if auth.IAM {
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("load default AWS config: %w", err)
		}

		// Create an AWS request Signer and load AWS configuration using
		// default config folder or env vars.
		signer, err := awsv2.NewSignerWithService(awsCfg, "es")
		if err != nil {
			return nil, fmt.Errorf("create request signer: %w", err)
		}

		osConfig.Signer = signer
	} else if username != "" {
		osConfig.Username = username
		osConfig.Password = password
	}

	searchClient, err := opensearch.NewClient(osConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"create opensearch client: %w", err)
	}

	return searchClient, nil
}
