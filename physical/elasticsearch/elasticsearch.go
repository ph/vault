package elasticsearch

import (
	"bytes"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/vault/helper/jsonutil"
	"github.com/hashicorp/vault/helper/strutil"
	"github.com/hashicorp/vault/physical"
	log "github.com/mgutz/logxi/v1"
)

const (
	defaultIndex             = ".vault" // Default Elasticsearch inddex to store the encrypted data
	defaultElasticsearchHost = "http://localhost:9200"
)

// Default template to make sure we set the key and the value to keyword type
// for later retrieval of namespaced values
var defaultTemplate = map[string]interface{}{
	"index_patterns": []string{".vault"},
	"mappings": map[string]interface{}{
		"doc": map[string]interface{}{
			"properties": map[string]interface{}{
				"key": map[string]interface{}{
					"type": "keyword",
				},
				"value": map[string]interface{}{
					"type": "keyword",
				},
				"prefix_bucket": map[string]interface{}{
					"type": "keyword",
				},
			},
		}},
}

type elasticsearchDocument struct {
	Key          string   `json:"key"`
	Value        []byte   `json:"value"`
	PrefixBucket []string `json:"prefix_bucket"`
}

type elasticsearchDocumentResponse struct {
	ID     string                `json:"_id"`
	Source elasticsearchDocument `json:"_source"`
}

type elasticsearchQueryResponse struct {
	Hits struct {
		Total int                             `json:"total"`
		Hits  []elasticsearchDocumentResponse `json:"hits"`
	} `json:"hits"`
}

func newElasticsearchDocumentFromEntry(entry *physical.Entry) *elasticsearchDocument {
	return &elasticsearchDocument{
		Key:          entry.Key,
		Value:        entry.Value,
		PrefixBucket: physical.Prefixes(entry.Key),
	}
}

func newEntryFromElasticsearchDocument(document elasticsearchDocument) *physical.Entry {
	return &physical.Entry{
		Key:   document.Key,
		Value: document.Value,
	}
}

type ElasticsearchBackend struct {
	client       *ElasticsearchClient
	logger       log.Logger
	index        string
	documentType string
}

func NewElasticsearchBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	host, ok := conf["host"]

	if !ok {
		host = defaultElasticsearchHost
	}

	index, ok := conf["index"]
	if !ok {
		index = defaultIndex
	}

	log.Debug("Connecting to: %s", host)

	tls, err := generateTLSConfig(conf)

	if err != nil {
		return nil, err
	}

	client, err := NewElasticsearchClient([]string{host}, tls, logger)

	if err != nil {
		return nil, fmt.Errorf("could not create the elasticsearch client, error: %s", err)
	}

	esBackend := &ElasticsearchBackend{
		client:       client,
		index:        index,
		logger:       logger,
		documentType: "doc",
	}

	manageTemplateConf, ok := conf["manage_template"]

	manageTemplate := false

	if ok {
		i, err := strconv.Atoi(manageTemplateConf)

		if err == nil && i == 1 {
			manageTemplate = true
		}
	} else {
		manageTemplate = true
	}

	if manageTemplate == true {
		err = esBackend.setup()

		if err != nil {
			return nil, err
		}
	}

	return esBackend, nil
}

func (b *ElasticsearchBackend) Put(entry *physical.Entry) error {
	defer metrics.MeasureSince([]string{"elasticsearch", "put"}, time.Now())
	b.logger.Debug("save key in elasticsearch", "key", entry.Key)

	jsonEncodedBytes, ok := jsonutil.EncodeJSON(newElasticsearchDocumentFromEntry(entry))

	if ok != nil {
		return fmt.Errorf("could not serialize to json  %v", entry.Key)
	}

	requestURL := b.generateURLEndpoint(b.encodeKeyForDocumentID(entry.Key))

	resp, err := b.client.Put(requestURL, bytes.NewReader(jsonEncodedBytes))

	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cannot create document in elasticsearch, status_code: %v", resp.StatusCode)
	}

	return err
}

func (b *ElasticsearchBackend) Get(key string) (*physical.Entry, error) {
	defer metrics.MeasureSince([]string{"elasticsearch", "get"}, time.Now())
	b.logger.Debug("retrieve value from elasticsearch", "key", key)

	requestURL := b.generateURLEndpoint(b.encodeKeyForDocumentID(key))

	resp, err := b.client.Get(requestURL, nil)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if len(resp.Body) == 0 {
		return nil, fmt.Errorf("receive an empty response from the server")
	}

	responseDocument := &elasticsearchDocumentResponse{}
	err = jsonutil.DecodeJSON(resp.Body, responseDocument)

	if err != nil {
		return nil, fmt.Errorf("could not unserialize document for key: %s", key)
	}

	entry := newEntryFromElasticsearchDocument(responseDocument.Source)

	return entry, nil
}

func (b *ElasticsearchBackend) Delete(key string) error {
	defer metrics.MeasureSince([]string{"elasticsearch", "delete"}, time.Now())
	b.logger.Debug("delete key", "key", key)

	requestURL := b.generateURLEndpoint(b.encodeKeyForDocumentID(key))

	resp, err := b.client.Delete(requestURL, nil)

	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("something went wrong when deleting the key")
	}

	return nil
}

func (b *ElasticsearchBackend) List(prefix string) ([]string, error) {
	defer metrics.MeasureSince([]string{"elasticsearch", "list"}, time.Now())

	b.logger.Debug("list", "prefix", prefix, "prefix_len", len(prefix))

	var searchQuery io.Reader

	if len(prefix) > 0 {
		reqBody := map[string]interface{}{
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					"prefix_bucket": b.normalizePrefix(prefix),
				},
			},
		}

		jsonEncodedBytes, err := jsonutil.EncodeJSON(reqBody)

		if err != nil {
			return nil, fmt.Errorf("could not serialize document")
		}

		b.logger.Debug("ES query", "query", string(jsonEncodedBytes))

		searchQuery = bytes.NewReader(jsonEncodedBytes)
	}

	resp, err := b.client.Get(b.searchURLEndpoint(), searchQuery)

	if err != nil {
		return nil, err
	}

	if len(resp.Body) == 0 {
		return nil, fmt.Errorf("empty response from the server")
	}

	responseDocument := &elasticsearchQueryResponse{}
	err = jsonutil.DecodeJSON(resp.Body, responseDocument)

	if err != nil {
		return nil, fmt.Errorf("count not unserialize the document")
	}

	if responseDocument.Hits.Total == 0 {
		return nil, nil
	}

	var keys []string

	for _, document := range responseDocument.Hits.Hits {
		var key string
		key = strings.TrimPrefix(document.Source.Key, prefix)

		if idx := strings.Index(key, "/"); idx == -1 {
			b.logger.Debug("adding a file", "file", key, "original_key", document.Source.Key)
			keys = append(keys, key)
		} else {
			b.logger.Debug("if Missing", "key", key[:idx+1], "original_key", document.Source.Key)
			keys = strutil.AppendIfMissing(keys, key[:idx+1])
		}
	}

	sort.Strings(keys)
	return keys, nil
}

func (b *ElasticsearchBackend) encodeKeyForDocumentID(key string) string {
	hash := sha1.New()
	io.WriteString(hash, key)
	return hex.EncodeToString(hash.Sum(nil))
}

func (b *ElasticsearchBackend) setup() error {
	jsonEncodedBytes, err := jsonutil.EncodeJSON(defaultTemplate)

	if err != nil {
		return err
	}

	resp, err := b.client.InstallTemplate(b.templateURLEndpoint(), bytes.NewReader(jsonEncodedBytes))

	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("could not install the template, status code: %s", resp.StatusCode)
	}

	return nil
}

func (b *ElasticsearchBackend) generateURLEndpoint(documentID string) string {
	return fmt.Sprintf("%s/%s/%s/?refresh", b.index, b.documentType, documentID)
}

func (b *ElasticsearchBackend) searchURLEndpoint() string {
	return fmt.Sprintf("%s/_search", b.index)
}

func (b *ElasticsearchBackend) templateURLEndpoint() string {
	return fmt.Sprintf("_template/%s", b.index)
}

func (b *ElasticsearchBackend) normalizePrefix(prefix string) string {
	if len(prefix) == 0 {
		return prefix
	}

	// Vault will send a prefix with this form `hello/hola/`, we need to remove the trailing /
	return prefix[0 : len(prefix)-1]
}

func generateTLSConfig(conf map[string]string) (*tls.Config, error) {
	var tlsConfig tls.Config

	tlsConf, ok := conf["tls_disable"]
	if ok {
		if i, err := strconv.Atoi(tlsConf); err == nil && i == 1 {
			return &tlsConfig, nil
		}
	}

	certificate, ok := conf["tls_cert_file"]

	if !ok {
		return nil, fmt.Errorf("missing configuration for 'tls_cert_file'")
	} else if _, err := os.Stat(certificate); err != nil {
		return nil, fmt.Errorf("the certificate file for 'tls_cert_file' doesn't exist, file: %s", certificate)
	}

	certificateKey, ok := conf["tls_key_file"]

	if !ok {
		return nil, fmt.Errorf("missing configuration for 'tls_key_file'")
	} else if _, err := os.Stat(certificate); err != nil {
		return nil, fmt.Errorf("the certificate key file for 'tls_key_file' doesn't exist, file: %s", certificateKey)
	}

	certificateKeyPair, err := tls.LoadX509KeyPair(certificate, certificateKey)

	if err != nil {
		return nil, err
	}

	ca, ok := conf["tls_client_ca_file"]
	var caPool *x509.CertPool

	if ok {
		caPem, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, err
		}

		caPool = x509.NewCertPool()
		ok := caPool.AppendCertsFromPEM(caPem)

		if !ok {
			return nil, fmt.Errorf("could not append root ca, please verify your file %s", ca)
		}
	}

	return &tls.Config{
		Certificates: []tls.Certificate{certificateKeyPair},
		RootCAs:      caPool,
	}, nil
}
