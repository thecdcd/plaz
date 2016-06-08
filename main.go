package main

import (
	"flag"
	"net"
	"os"

	"github.com/gogo/protobuf/proto"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
	. "github.com/thecdcd/plaz/scheduler"
	. "github.com/thecdcd/plaz/datalayer"
	"github.com/thecdcd/plaz/health"
	"net/http"
	"sync"
)

const (
	FRAMEWORK_NAME = "plaz"
	FRAMEWORK_USER = ""
)

var (
	address = flag.String("address", "127.0.0.1", "Binding address for artifact server")
	influxAddress = flag.String("influx-address", "http://localhost:8086", "URL for InfluxDB instance")
	influxDatabase = flag.String("influx-db", "mesos_resources", "InfluxDB database name to use for storing mesos events.")
	influxPassword = flag.String("influx-password", "", "Password for InfluxDB instance")
	influxUsername = flag.String("influx-username", "", "Username for InfluxDB instance")
	webPort = flag.String("web-port", "8080", "Port to use for HTTP health check listener.")
	master = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
)

func init() {
	flag.Parse()
}

func main() {
	dataConfig := NewDataConfig((*influxAddress), (*influxDatabase), (*influxUsername), (*influxPassword))
	// we want the data driver handled here based on config
	var dataDriver DataDriver
	if ((*influxAddress) != "") {
		dataDriver = NewInfluxClient(dataConfig)
	} else {
		log.Fatalln("Could not determine correct data driver to use")
		os.Exit(-3)
	}

	scheduler, err := NewPlazScheduler(dataDriver)
	if err != nil {
		log.Fatalf("Failed to create scheduler: ", err)
		os.Exit(-2)
	}

	// framework
	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(FRAMEWORK_USER), // mesos-go will fill this in
		Name: proto.String(FRAMEWORK_NAME),
	}

	// scheduler driver
	config := sched.DriverConfig{
		Scheduler: scheduler,
		Framework: fwinfo,
		Master: *master,
		Credential: (*mesos.Credential)(nil),
		BindingAddress: parseIP(*address),
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		// start health check
		healthMutex := http.NewServeMux()
		healthMutex.HandleFunc("/health", health.HealthCheckHandler)
		log.Infoln("Starting health check service on port", (*webPort))
		http.ListenAndServe(":" + (*webPort), healthMutex)
		log.Fatalf("Health check service shutdown.")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		driver, err := sched.NewMesosSchedulerDriver(config)
		if err != nil {
			log.Fatalf("Unable to create SchedulerDriver: ", err.Error())
			os.Exit(-3)
		}

		if stat, err := driver.Run(); err != nil {
			log.Fatalf("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
		}
		wg.Done()
	}()

	wg.Wait()
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Fatal(err)
	}
	if len(addr) < 1 {
		log.Fatalf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}
