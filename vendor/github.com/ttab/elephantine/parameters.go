package elephantine

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"
)

type ParameterSource interface {
	GetParameterValue(ctx context.Context, name string) (string, error)
}

type noParameterSource struct{}

func (noParameterSource) GetParameterValue(_ context.Context, _ string) (string, error) {
	return "", errors.New("no parameter source configured")
}

func GetParameterSource(name string) (ParameterSource, error) {
	switch name {
	case "":
		return noParameterSource{}, nil
	case "ssm":
		return NewLazySSM(), nil
	default:
		return nil, fmt.Errorf("unknown parameter source %q", name)
	}
}

func ResolveParameter(
	c *cli.Context, src ParameterSource, name string,
) (string, error) {
	paramName := c.String(name + "-parameter")
	if paramName == "" {
		return c.String(name), nil
	}

	value, err := src.GetParameterValue(c.Context, paramName)
	if err != nil {
		return "", fmt.Errorf("failed to fetch %q (%s) parameter value: %w",
			paramName, name, err)
	}

	return value, nil
}
