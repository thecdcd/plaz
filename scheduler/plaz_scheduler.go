package scheduler

import (
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
)

type PlazScheduler struct {
	influx *InfluxClient
}

func NewPlazScheduler(conf *InfluxConfig) (*PlazScheduler, error) {
	client := NewInfluxClient(conf)
	return &PlazScheduler{
		influx: client,
	}, nil
}

func (sched *PlazScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Registered with Master ", masterInfo)
	sched.influx.Connect()
}

func (sched *PlazScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Re-Registered with Master ", masterInfo)
}

func (sched *PlazScheduler) Disconnected(sched.SchedulerDriver) {
	log.Infoln("Scheduler Disconnected")
	sched.influx.Close()
}

func (sched *PlazScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	logOffers(offers)

	bp, err := sched.influx.BatchPoint()
	if err != nil {
		log.Warningln("failed to create batch point")
	}

	for _, offer := range offers {
		driver.DeclineOffer(offer.GetId(), &mesos.Filters{})
		log.Infof("Declined offer %v from %v\n", offer.GetId().GetValue(), offer.SlaveId.GetValue())

		tags, fields := offerToPoint(offer)
		err := sched.influx.AddPoint(bp, "offer", tags, fields)
		if err != nil {
			log.Warningln("Failed to add point")
		}
	}

	if bp != nil {
		sched.influx.WriteBatch(bp)
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
	addSinglePointBatch(sched.influx, tags, fields)
}

func (sched *PlazScheduler) ExecutorLost(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, i int) {
	log.Infof("Executor '%v' lost on slave '%v' with exit code: %v.\n", *exId, *slvId, i)
	tags, fields := execLostToPoint(exId, slvId, i)
	addSinglePointBatch(sched.influx, tags, fields)
}

func (sched *PlazScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error: ", err)
}

func addSinglePointBatch(client *InfluxClient, tags PointTags, fields PointFields) {
	bp, err := client.BatchPoint()
	if err == nil {
		err = client.AddPoint(bp, "execlost", tags, fields)
		if err != nil {
			log.Warningln("Failed to add point: ", err)
		}
		client.WriteBatch(bp)
	} else {
		log.Warningln("failed to create batch point: ", err)
	}
}