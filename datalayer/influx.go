package datalayer

import (
	"fmt"
	"time"
	"math/rand"

	log "github.com/golang/glog"
	influx "github.com/influxdata/influxdb/client/v2"
)

type InfluxConfig struct {
	address  string
	username string
	password string
	database string
}

type InfluxClient struct {
	config  *InfluxConfig
	client  influx.Client
	batches map[string]influx.BatchPoints
}

func NewInfluxConfig(address string, database string, username string, password string) *InfluxConfig {
	return &InfluxConfig{
		database: database,
		username: username,
		password: password,
		address: address,
	}
}

func NewInfluxClient(config *InfluxConfig) *InfluxClient {
	return &InfluxClient{
		config: config,
		batches: make(map[string]influx.BatchPoints),
	}
}

func (client *InfluxClient) Connect() error {
	// Make client
	config := influx.HTTPConfig{
		Addr: client.config.address,
	}
	if client.config.username != "" {
		config.Username = client.config.username
	}
	if client.config.password != "" {
		config.Password = client.config.password
	}

	c, err := influx.NewHTTPClient(config)

	if err != nil {
		return err
	}

	client.client = c
	log.Infoln("Connected to InfluxDB at", client.config.address)

	_, err = client.QueryDb(fmt.Sprintf("CREATE DATABASE %s", client.config.database))
	if err != nil {
		return err
	}

	return nil
}

func (client *InfluxClient) QueryDb(cmd string) (res []influx.Result, err error) {
	q := influx.Query{
		Command:  cmd,
		Database: client.config.database,
	}
	if response, err := client.client.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}

	return res, nil
}

func (client *InfluxClient) StartBatch() (string, error) {
	// Create a new point batch
	batches, err := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  client.config.database,
		Precision: "s",
	})
	if err != nil {
		return "", err
	}
	session := sessionId()
	client.batches[session] = batches
	return session, nil
}

func (client *InfluxClient) RecordPoint(session string, data DataFields) error {
	pt, err := influx.NewPoint(data["name"].(string), data["tags"].(DataTags), data["fields"].(DataFields), time.Now())
	if err != nil {
		return err
	}
	client.batches[session].AddPoint(pt)
	return nil
}

func (client *InfluxClient) WriteBatch(session string) error {
	err := client.client.Write(client.batches[session])
	delete(client.batches, session)
	return err
}

func (client *InfluxClient) Close() error {
	return client.client.Close()
}

func (client *InfluxClient) CreatePoint(name string, tags DataTags, fields DataFields) DataFields {
	return DataFields{
		"name": name,
		"tags": tags,
		"fields": fields,
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1 << letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// stolen from "icza"
// http://stackoverflow.com/a/31832326
func sessionId() string {
	n := 6
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n - 1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}