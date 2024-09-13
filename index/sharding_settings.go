package index

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type ShardingPolicy struct {
	Default ShardingSettings
	Indexes []IndexSharding
}

type ShardingSettings struct {
	Shards   int `json:"number_of_shards"`
	Replicas int `json:"number_of_replicas"`
}

type IndexSharding struct {
	Prefix   string
	Settings ShardingSettings
}

func ParseShardingPolicy(
	value string, defaultSettings ShardingSettings,
) (ShardingPolicy, error) {
	conf := ShardingPolicy{
		Default: defaultSettings,
	}

	if value == "" {
		return conf, nil
	}

	stanzas := strings.Split(value, ",")

	for _, stanza := range stanzas {
		parts := strings.Split(stanza, ":")
		if len(parts) != 3 {
			return ShardingPolicy{}, fmt.Errorf(
				"invalid stanza: %q", stanza)
		}

		shards, err := strconv.Atoi(parts[1])
		if err != nil {
			return ShardingPolicy{}, fmt.Errorf(
				"invalid stanza shard count: %q", stanza)
		}

		if shards == 0 {
			return ShardingPolicy{}, fmt.Errorf(
				"invalid stanza, shards cannot be 0: %q", stanza)
		}

		replicas, err := strconv.Atoi(parts[2])
		if err != nil {
			return ShardingPolicy{}, fmt.Errorf(
				"invalid stanza replica count: %q", stanza)
		}

		if parts[0] == "" {
			conf.Default.Shards = shards
			conf.Default.Replicas = replicas
		} else {
			conf.Indexes = append(conf.Indexes, IndexSharding{
				Prefix: parts[0],
				Settings: ShardingSettings{
					Shards:   shards,
					Replicas: replicas,
				},
			})
		}
	}

	slices.SortFunc(conf.Indexes, func(a, b IndexSharding) int {
		return len(b.Prefix) - len(a.Prefix)
	})

	return conf, nil
}

func (sc ShardingPolicy) GetSettings(index string) ShardingSettings {
	for _, s := range sc.Indexes {
		if strings.HasPrefix(index, s.Prefix) {
			return s.Settings
		}
	}

	return sc.Default
}
