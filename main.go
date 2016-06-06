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
)

var (
	address = flag.String("address", "127.0.0.1", "Binding address for artifact server")
	influxAddress = flag.String("influx-address", "http://localhost:8086", "URL for InfluxDB instance")
	influxDatabase = flag.String("influx-db", "mesos_resources", "InfluxDB database name to use for storing mesos events.")
	influxPassword = flag.String("influx-password", "", "Password for InfluxDB instance")
	influxUsername = flag.String("influx-username", "", "Username for InfluxDB instance")
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
		User: proto.String(""), // mesos-go will fill this in
		Name: proto.String("plaz"),
	}

	// scheduler driver
	config := sched.DriverConfig{
		Scheduler: scheduler,
		Framework: fwinfo,
		Master: *master,
		Credential: (*mesos.Credential)(nil),
		BindingAddress: parseIP(*address),
	}

	driver, err := sched.NewMesosSchedulerDriver(config)
	if err != nil {
		log.Fatalf("Unable to create SchedulerDriver: ", err.Error())
		os.Exit(-3)
	}

	if stat, err := driver.Run(); err != nil {
		log.Fatalf("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}
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
