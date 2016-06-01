package scheduler

import (
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

type PointTags map[string]string
type PointFields map[string]interface{}

func getOfferScalar(offer *mesos.Offer, name string) float64 {
	resources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == name
	})

	value := 0.0
	for _, res := range resources {
		value += res.GetScalar().GetValue()
	}

	return value
}

func getOfferCpu(offer *mesos.Offer) float64 {
	return getOfferScalar(offer, "cpus")
}

func getOfferMem(offer *mesos.Offer) float64 {
	return getOfferScalar(offer, "mem")
}

func getOfferDisk(offer *mesos.Offer) float64 {
	return getOfferScalar(offer, "disk")
}

func logOffers(offers []*mesos.Offer) {
	for _, offer := range offers {
		log.Infof("Received Offer <%v> with cpues=%v mem=%v", offer.Id.GetValue(), getOfferCpu(offer), getOfferMem(offer))
	}
}

func offerToPoint(offer *mesos.Offer) (PointTags, PointFields) {
	tags := PointTags{
		"slave": offer.SlaveId.GetValue(),
		"hostname": *offer.Hostname,
	}
	fields := PointFields{
		"cpus": getOfferCpu(offer),
		"mem": getOfferMem(offer),
		"disk": getOfferDisk(offer),
	}
	return tags, fields
}

func execLostToPoint(exId *mesos.ExecutorID, slvId *mesos.SlaveID, exitCode int) (PointTags, PointFields) {
	tags := PointTags{"slave": slvId.GetValue()}
	fields := PointFields{
		"executor": exId.GetValue(),
		"exitcode": exitCode,
	}
	return tags, fields
}

func slaveLostToPoint(slvId *mesos.SlaveID) (PointTags, PointFields) {
	fields := PointFields{
		"slave": slvId.GetValue(),
	}
	return nil, fields
}