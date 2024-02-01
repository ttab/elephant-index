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
	IAM bool
}

type OSClientProvider struct {
	clusters ClusterGetter
}

func NewOSClientProvider(clusters ClusterGetter) *OSClientProvider {
	return &OSClientProvider{
		clusters: clusters,
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
	}

	searchClient, err := opensearch.NewClient(osConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"create opensearch client: %w", err)
	}

	return searchClient, nil
}
