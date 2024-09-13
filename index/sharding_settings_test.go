package index_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephantine/test"
)

type shardTestCase struct {
	Config index.ShardingPolicy
	Probes map[string]index.ShardingSettings
}

func TestShardingSettings(t *testing.T) {
	samples := map[string]shardTestCase{
		":1:2,core_article-:2:2,core_article-sv-se:5:2,tt_:3:2": {
			Config: index.ShardingPolicy{
				Default: index.ShardingSettings{
					Shards:   1,
					Replicas: 2,
				},
				Indexes: []index.ShardingRule{
					{
						Prefix: "core_article-sv-se",
						Settings: index.ShardingSettings{
							Shards:   5,
							Replicas: 2,
						},
					},
					{
						Prefix: "core_article-",
						Settings: index.ShardingSettings{
							Shards:   2,
							Replicas: 2,
						},
					},
					{
						Prefix: "tt_",
						Settings: index.ShardingSettings{
							Shards:   3,
							Replicas: 2,
						},
					},
				},
			},
			Probes: map[string]index.ShardingSettings{
				"core_article-en-us": {
					Shards:   2,
					Replicas: 2,
				},
				"core_category-sv-se": {
					Shards:   1,
					Replicas: 2,
				},
				"core_article-sv-se": {
					Shards:   5,
					Replicas: 2,
				},
				"tt_grattis-sv-se": {
					Shards:   3,
					Replicas: 2,
				},
			},
		},
	}

	for input, c := range samples {
		got, err := index.ParseShardingPolicy(input, index.ShardingSettings{
			Shards:   4,
			Replicas: 2,
		})
		test.Must(t, err, "parse sharding configuration")

		if diff := cmp.Diff(c.Config, got); diff != "" {
			t.Errorf("config mismatch (-want +got):\n%s", diff)
		}

		for probe, wantSetting := range c.Probes {
			gotSetting := got.GetSettings(probe)

			if diff := cmp.Diff(wantSetting, gotSetting); diff != "" {
				t.Errorf("setting mismatch for %q (-want +got):\n%s",
					probe, diff)
			}
		}
	}
}
