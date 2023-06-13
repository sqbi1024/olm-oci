package remote

import (
	"context"
	"sync"

	"github.com/containers/image/v5/docker/reference"
	"github.com/containers/image/v5/pkg/docker/config"
	"github.com/docker/cli/cli/config/configfile"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
)

var (
	loadOnce      sync.Once
	dockerCfg     *configfile.ConfigFile
	loadConfigErr error
)

func getCredentials(repoName string) func(context.Context, string) (auth.Credential, error) {
	return func(ctx context.Context, _ string) (auth.Credential, error) {
		ref, err := reference.ParseNamed(repoName)
		if err != nil {
			return auth.Credential{}, err
		}
		authConfig, err := config.GetCredentialsForRef(nil, ref)
		if err != nil {
			return auth.Credential{}, err
		}
		return auth.Credential{
			Username: authConfig.Username,
			Password: authConfig.Password,
		}, nil
	}
}

func NewRepository(repoName string) (*remote.Repository, error) {
	repo, err := remote.NewRepository(repoName)
	if err != nil {
		return nil, err
	}
	repo.Client = &auth.Client{Credential: getCredentials(repoName)}
	return repo, nil
}
