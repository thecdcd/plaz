# plaz

> Synthetic glass, used for windows (especially in aircraft and spaceships) due to its superior strength. -- [Dune Glossary via Wikipedia](https://en.wikipedia.org/wiki/List_of_Dune_terminology#P)

`plaz` is the window to your Mesos and DC/OS installation. It is a framework that monitors Mesos events and logs them to a datasource. At this time, the only available data source is [InfluxDB](https://influxdata.com/).

## Building and Running

    git clone https://github.com/thecdcd/plaz
    cd plaz && go build -o plaz_scheduler
    ./plaz_scheduler --help

## Marathon Integration
`plaz` comes with a health check endpoint that can be used with Marathon to ensure the service is always running.
