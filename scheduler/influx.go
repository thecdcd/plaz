package scheduler

import (
	"fmt"
	log "github.com/golang/glog"

	influx "github.com/influxdata/influxdb/client/v2"
	"time"
)

type InfluxConfig struct {
	address  string
	username string
	password string
	database string
}

type InfluxClient struct {
	config *InfluxConfig
	client influx.Client
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
	}
}

func (client *InfluxClient) Connect() {
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
		log.Fatalln("Error: ", err)
	}

	client.client = c
	log.Infoln("Connected to InfluxDB at", client.config.address)

	_, err = client.QueryDb(fmt.Sprintf("CREATE DATABASE %s", client.config.database))
	if err != nil {
		log.Fatal(err)
	}
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

func (client *InfluxClient) BatchPoint() (influx.BatchPoints, error) {
	// Create a new point batch
	return influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  client.config.database,
		Precision: "s",
	})
}

func (client *InfluxClient) AddPoint(batch influx.BatchPoints, name string, tags map[string]string, fields map[string]interface{}) error {
	pt, err := influx.NewPoint(name, tags, fields, time.Now())
	if err != nil {
		return err
	}
	batch.AddPoint(pt)
	return nil
}

func (client *InfluxClient) WriteBatch(batch influx.BatchPoints) error {
	return client.client.Write(batch)
}

func (client *InfluxClient) Close() error {
	return client.client.Close()
}

