package elasticsearch

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	log "github.com/mgutz/logxi/v1"
)

type ElasticsearchResponse struct {
	StatusCode int
	Body       []byte
}

type ElasticsearchClient struct {
	hosts      []string
	logger     log.Logger
	httpClient *http.Client
}

const (
	headerUserAgent = "user-agent"
	userAgent       = "Vault"

	headerContentType = "content-type"
	contentType       = "application/json"
)

// TODO: Uses context package to wrap the calls and manage expectation
func NewElasticsearchClient(hosts []string, tlsConfig *tls.Config, logger log.Logger) (*ElasticsearchClient, error) {
	if len(hosts) == 0 {
		return nil, fmt.Errorf("you need to specify at least one elasticsearch host")
	}

	return &ElasticsearchClient{
		hosts:  hosts,
		logger: logger,
		httpClient: &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				TLSClientConfig:       tlsConfig,
			},
		},
	}, nil
}

func (c *ElasticsearchClient) InstallTemplate(requestURL string, reader io.Reader) (*ElasticsearchResponse, error) {
	return c.doRequest(http.MethodPut, requestURL, reader)
}

func (c *ElasticsearchClient) Put(requestURL string, reader io.Reader) (*ElasticsearchResponse, error) {
	return c.doRequest(http.MethodPut, requestURL, reader)
}

func (c *ElasticsearchClient) Get(requestURL string, reader io.Reader) (*ElasticsearchResponse, error) {
	return c.doRequest(http.MethodGet, requestURL, reader)
}

func (c *ElasticsearchClient) Delete(requestURL string, reader io.Reader) (*ElasticsearchResponse, error) {
	return c.doRequest(http.MethodDelete, requestURL, reader)
}

func (c *ElasticsearchClient) doRequest(verb string, url string, reader io.Reader) (*ElasticsearchResponse, error) {
	c.logger.Debug("do request", "verb", verb, "url", url)

	url = c.addHostToRequest(url)

	req, err := http.NewRequest(verb, url, reader)
	if err != nil {
		return nil, fmt.Errorf("malformed request")
	}

	req.Header.Add(headerUserAgent, userAgent)
	req.Header.Add(headerContentType, contentType)

	resp, err := c.httpClient.Do(req)

	if err != nil {
		return nil, fmt.Errorf("problem when querying the remote host, verify your host information or your credentials")
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("permission denied to access the elasticsearch cluster, verify your credentials or your permissions")
	}

	stringContent, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, fmt.Errorf("could not read content from server response")
	}

	return &ElasticsearchResponse{
		StatusCode: resp.StatusCode,
		Body:       stringContent,
	}, nil
}

func (c *ElasticsearchClient) addHostToRequest(requestURL string) string {
	return fmt.Sprintf("%s/%s", c.getHost(), requestURL)
}

func (c *ElasticsearchClient) getHost() string {
	return c.hosts[rand.Intn(len(c.hosts))]
}
