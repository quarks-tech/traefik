package acme

import (
	"encoding/json"
	"fmt"

	"github.com/hashicorp/consul/api"
)

type ConsulConfig struct {
	Endpoints []string
	Token     string
	RootKey   string
}

type ConsulStore struct {
	rootKey string
	client  *api.Client
}

func NewConsulStore(consulACME ConsulConfig) (*ConsulStore, error) {
	if len(consulACME.Endpoints) == 0 {
		return nil, fmt.Errorf("consul endpoints are not provided")
	}

	config := api.Config{
		Address: consulACME.Endpoints[0],
		Scheme:  "http",
		Token:   consulACME.Token,
	}

	client, err := api.NewClient(&config)
	if err != nil {
		return nil, err
	}

	if consulACME.RootKey == "" {
		consulACME.RootKey = "traefik"
	}

	return &ConsulStore{
		client:  client,
		rootKey: "traefik-acme",
	}, nil
}

func (s *ConsulStore) withLock(key string, fn func() error) error {
	lock, err := s.client.LockKey(key)
	if err != nil {
		return fmt.Errorf("consul: lock key %s: %v", key, err)
	}

	if _, err = lock.Lock(nil); err != nil {
		return fmt.Errorf("consul: lock %s: %v", key, err)
	}

	defer func() {
		_ = lock.Unlock()
	}()

	return fn()
}

func (s *ConsulStore) resolverLockKey(resolverName string) string {
	return s.rootKey + "/acme/locks/" + resolverName
}

func (s *ConsulStore) accountKey(resolverName string) string {
	return s.rootKey + "/acme/account/" + resolverName
}

func (s *ConsulStore) certificatesKey(resolverName string) string {
	return s.rootKey + "/acme/certs/" + resolverName
}

func (s *ConsulStore) GetAccount(resolverName string) (*Account, error) {
	var account Account

	err := s.withLock(s.resolverLockKey(resolverName), func() error {
		pair, _, err := s.client.KV().Get(s.accountKey(resolverName), nil)
		if err != nil {
			return err
		}

		if pair == nil {
			return nil
		}

		if err := json.Unmarshal(pair.Value, &account); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &account, nil
}

func (s *ConsulStore) SaveAccount(resolverName string, account *Account) error {
	return s.withLock(s.resolverLockKey(resolverName), func() error {
		data, err := json.Marshal(account)
		if err != nil {
			return err
		}

		pair := &api.KVPair{
			Key:   s.accountKey(resolverName),
			Value: data,
		}

		_, err = s.client.KV().Put(pair, nil)

		return err
	})
}

func (s *ConsulStore) GetCertificates(resolverName string) ([]*CertAndStore, error) {
	var certs []*CertAndStore

	err := s.withLock(s.resolverLockKey(resolverName), func() error {
		pairs, _, err := s.client.KV().List(s.certificatesKey(resolverName)+"/", nil)
		if err != nil {
			return err
		}

		for _, p := range pairs {
			var cert CertAndStore

			if err := json.Unmarshal(p.Value, &cert); err != nil {
				return err
			}

			certs = append(certs, &cert)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return certs, err
}

func (s *ConsulStore) SaveCertificates(resolverName string, certificates []*CertAndStore) error {
	return s.withLock(s.resolverLockKey(resolverName), func() error {
		certsTree := s.certificatesKey(resolverName) + "/"

		if _, err := s.client.KV().DeleteTree(certsTree, nil); err != nil {
			return err
		}

		for _, c := range certificates {
			data, err := json.Marshal(c)
			if err != nil {
				return err
			}

			pair := &api.KVPair{
				Key:   certsTree + c.Domain.Main,
				Value: data,
			}

			if _, err = s.client.KV().Put(pair, nil); err != nil {
				return err
			}
		}

		return nil
	})
}
