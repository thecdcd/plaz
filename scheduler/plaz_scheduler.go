package scheduler

import (
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
	util "github.com/mesos/mesos-go/mesosutil"
	. "github.com/thecdcd/plaz/datalayer"
	"github.com/thecdcd/plaz/health"
)

type PlazScheduler struct {
	client DataDriver
}

func NewPlazScheduler(driver DataDriver) *PlazScheduler {
	return &PlazScheduler{
		client: driver,
	}
}

func (sched *PlazScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Registered with Master ", masterInfo)
	health.HealthStatus.Scheduler = true
}

func (sched *PlazScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Re-Registered with Master ", masterInfo)
	health.HealthStatus.Scheduler = true
}

func (sched *PlazScheduler) Disconnected(sched.SchedulerDriver) {
	log.Infoln("Scheduler Disconnected")
	health.HealthStatus.Scheduler = false
}

func (sched *PlazScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	logOffers(offers)

	bp, err := sched.client.StartBatch()
	if err == nil {
		for _, offer := range offers {
			driver.DeclineOffer(offer.GetId(), &mesos.Filters{})
			tags, fields := offerToPoint(offer)
			pt := sched.client.CreatePoint(OFFER_NAME, tags, fields)
			err := sched.client.RecordPoint(bp, pt)
			if err != nil {
				log.Warningln("Failed to add point")
			}
		}

		sched.client.WriteBatch(bp)

	} else {

		log.Warningln("failed to create batch point")
	}
}

func (sched *PlazScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task ", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())

	if status.GetState() == mesos.TaskState_TASK_LOST ||
	status.GetState() == mesos.TaskState_TASK_KILLED ||
	status.GetState() == mesos.TaskState_TASK_FAILED {
		log.Infoln("Aborting because task ", status.TaskId.GetValue(),
			" is in unexpected state ", status.State.String(),
			" with message ", status.GetMessage())
	}
	driver.Abort()
}

func (sched *PlazScheduler) OfferRescinded(s sched.SchedulerDriver, id *mesos.OfferID) {
	log.Infof("Offer '%v' rescinded.\n", *id)
}

func (sched *PlazScheduler) FrameworkMessage(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, msg string) {
	log.Infof("Received framework message from executor '%v' on slave '%v': %s.\n", *exId, *slvId, msg)
}

func (sched *PlazScheduler) SlaveLost(s sched.SchedulerDriver, id *mesos.SlaveID) {
	log.Infof("Slave '%v' lost.\n", *id)
	tags, fields := slaveLostToPoint(id)
	addSinglePointBatch(sched.client, SLAVE_LOST_NAME, tags, fields)
}

func (sched *PlazScheduler) ExecutorLost(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, i int) {
	log.Infof("Executor '%v' lost on slave '%v' with exit code: %v.\n", *exId, *slvId, i)
	tags, fields := execLostToPoint(exId, slvId, i)
	addSinglePointBatch(sched.client, EXECUTOR_LOST_NAME, tags, fields)
}

func (sched *PlazScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error: ", err)
}

func addSinglePointBatch(client DataDriver, name string, tags DataTags, fields DataFields) {
	bp, err := client.StartBatch()
	if err == nil {
		pt := client.CreatePoint(name, tags, fields)
		err = client.RecordPoint(bp, pt)
		if err != nil {
			log.Warningln("Failed to add point: ", err)
		}
		client.WriteBatch(bp)
	} else {
		log.Warningln("failed to create batch point: ", err)
	}
}

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

func offerToPoint(offer *mesos.Offer) (DataTags, DataFields) {
	tags := DataTags{
		"slave": offer.SlaveId.GetValue(),
		"hostname": *offer.Hostname,
	}
	fields := DataFields{
		"cpus": getOfferCpu(offer),
		"mem": getOfferMem(offer),
		"disk": getOfferDisk(offer),
	}
	return tags, fields
}

func execLostToPoint(exId *mesos.ExecutorID, slvId *mesos.SlaveID, exitCode int) (DataTags, DataFields) {
	tags := DataTags{"slave": slvId.GetValue()}
	fields := DataFields{
		"executor": exId.GetValue(),
		"exitcode": exitCode,
	}
	return tags, fields
}

func slaveLostToPoint(slvId *mesos.SlaveID) (DataTags, DataFields) {
	fields := DataFields{
		"slave": slvId.GetValue(),
	}
	return nil, fields
}