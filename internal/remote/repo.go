package remote

import (
	"context"

	"github.com/docker/cli/cli/config"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
)

func defaultCredentials(ctx context.Context, registry string) (auth.Credential, error) {
	cfg, err := config.Load(config.Dir())
	if err != nil {
		return auth.Credential{}, err
	}
	authConfig, err := cfg.GetAuthConfig(registry)
	if err != nil {
		return auth.Credential{}, err
	}
	return auth.Credential{
		Username: authConfig.Username,
		Password: authConfig.Password,
	}, nil
}

func NewRepository(repoName string) (*remote.Repository, error) {
	repo, err := remote.NewRepository(repoName)
	if err != nil {
		return nil, err
	}
	repo.Client = &auth.Client{Credential: defaultCredentials}
	return repo, nil
}
