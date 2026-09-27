//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination rescheduler_mock.go

package queues

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/common/util"
)

const (
	taskChanFullBackoff                  = 2 * time.Second
	taskChanFullBackoffJitterCoefficient = 0.5

	reschedulerPQCleanupDuration          = 3 * time.Minute
	reschedulerPQCleanupJitterCoefficient = 0.15

	budgetRetryJitterCoefficient = 0.5
)

type (
	// Rescheduler buffers task executables that are failed to process and
	// resubmit them to the task scheduler when the Reschedule method is called.
	Rescheduler interface {
		// Add task executable to the rescheduler. throttle is the budget it is waiting on, or
		// the zero key when the controller does not pace it.
		Add(task Executable, rescheduleTime time.Time, throttle ThrottleKey)

		// Reschedule triggers an immediate reschedule for provided namespace
		// ignoring executable's reschedule time.
		// Used by namespace failover logic
		Reschedule(namespaceID string)

		// Len returns the total number of task executables waiting to be rescheduled.
		Len() int
		Start()
		Stop()
	}

	rescheduledExecuable struct {
		executable     Executable
		rescheduleTime time.Time
	}

	// The zero ThrottleKey marks work that is not governed by the controller.
	reschedulerKey struct {
		TaskChannelKey
		Throttle ThrottleKey
	}

	reschedulerImpl struct {
		scheduler      Scheduler
		timeSource     clock.TimeSource
		logger         log.Logger
		metricsHandler metrics.Handler
		throttleState  *ThrottleState

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		timerGate        timer.Gate
		taskChannelKeyFn TaskChannelKeyFn

		sync.Mutex
		pqMap          map[reschedulerKey]collection.Queue[rescheduledExecuable]
		numExecutables int
	}
)

func NewRescheduler(
	scheduler Scheduler,
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsHandler metrics.Handler,
	throttleState *ThrottleState,
) *reschedulerImpl {
	return &reschedulerImpl{
		scheduler:      scheduler,
		timeSource:     timeSource,
		logger:         logger,
		metricsHandler: metricsHandler,
		throttleState:  throttleState,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		timerGate:        timer.NewLocalGate(timeSource),
		taskChannelKeyFn: scheduler.TaskChannelKeyFn(),

		pqMap: make(map[reschedulerKey]collection.Queue[rescheduledExecuable]),
	}
}

func (r *reschedulerImpl) Start() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	r.shutdownWG.Add(1)
	go r.rescheduleLoop()

	r.logger.Info("Task rescheduler started.", tag.LifeCycleStarted)
}

func (r *reschedulerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(r.shutdownCh)
	r.timerGate.Close()

	if success := common.AwaitWaitGroup(&r.shutdownWG, time.Minute); !success {
		r.logger.Warn("Task rescheduler timedout on shutdown.", tag.LifeCycleStopTimedout)
	}

	r.logger.Info("Task rescheduler stopped.", tag.LifeCycleStopped)
}

func (r *reschedulerImpl) Add(
	executable Executable,
	rescheduleTime time.Time,
	throttle ThrottleKey,
) {
	key := reschedulerKey{TaskChannelKey: r.taskChannelKeyFn(executable), Throttle: throttle}

	r.Lock()
	pq := r.getOrCreateClassLocked(key)
	pq.Add(rescheduledExecuable{
		executable:     executable,
		rescheduleTime: rescheduleTime,
	})
	r.numExecutables++
	r.timerGate.Update(rescheduleTime)
	r.Unlock()

	if r.isStopped() {
		r.drain()
	}
}

func (r *reschedulerImpl) Reschedule(
	namespaceID string,
) {
	r.Lock()
	defer r.Unlock()

	now := r.timeSource.Now()
	updatedRescheduleTime := false
	for key, pq := range r.pqMap {
		if key.NamespaceID != namespaceID {
			continue
		}

		updatedRescheduleTime = true
		// set reschedule time for all tasks in this pq to be now
		items := make([]rescheduledExecuable, 0, pq.Len())
		for !pq.IsEmpty() {
			rescheduled := pq.Remove()
			// scheduled queue pre-fetches tasks,
			// so we need to make sure the reschedule time is not before the task scheduled time
			rescheduled.rescheduleTime = util.MaxTime(
				rescheduled.executable.GetKey().FireTime.Add(common.ScheduledTaskMinPrecision),
				now,
			)
			items = append(items, rescheduled)
		}
		r.pqMap[key] = r.newPriorityQueue(items)
	}

	// then update timer gate to trigger the actual reschedule
	if updatedRescheduleTime {
		r.timerGate.Update(now)
	}
}

func (r *reschedulerImpl) Len() int {
	r.Lock()
	defer r.Unlock()

	return r.numExecutables
}

func (r *reschedulerImpl) rescheduleLoop() {
	defer r.shutdownWG.Done()

	cleanupTimer := time.NewTimer(backoff.Jitter(
		reschedulerPQCleanupDuration,
		reschedulerPQCleanupJitterCoefficient,
	))
	defer cleanupTimer.Stop()

	for {
		select {
		case <-r.shutdownCh:
			r.drain()
			return
		case <-r.timerGate.FireCh():
			r.reschedule()
		case <-cleanupTimer.C:
			r.cleanupPQ()
			cleanupTimer.Reset(backoff.Jitter(
				reschedulerPQCleanupDuration,
				reschedulerPQCleanupJitterCoefficient,
			))
		}
	}

}

type reschedulePass struct {
	now      time.Time
	nextWake time.Time
}

func (p *reschedulePass) wakeAt(t time.Time) {
	if p.nextWake.IsZero() || t.Before(p.nextWake) {
		p.nextWake = t
	}
}

func (r *reschedulerImpl) reschedule() {
	r.Lock()
	defer r.Unlock()

	metrics.TaskReschedulerPendingTasks.With(r.metricsHandler).Record(int64(r.numExecutables))
	pass := reschedulePass{now: r.timeSource.Now()}

	// One pass per priority band, most urgent first. Inside a band the map's order decides,
	// and that differs from pass to pass, so classes of equal priority take turns.
	for band := 0; band < numPriorityBands; band++ {
		for key, pq := range r.pqMap {
			if priorityBand(key.Priority) != band || pq.IsEmpty() {
				continue
			}
			r.drainClassLocked(key, pq, &pass)
		}
	}

	if !pass.nextWake.IsZero() {
		r.timerGate.Update(pass.nextWake)
	}
}

// One band per named priority, plus a last one so a priority nobody named still drains
// instead of sticking in its queue forever.
var numPriorityBands = len(ctasks.PriorityOrder) + 1

func priorityBand(p ctasks.Priority) int {
	if band := slices.Index(ctasks.PriorityOrder, p); band >= 0 {
		return band
	}
	return len(ctasks.PriorityOrder) // unnamed, so after every named band
}

func (r *reschedulerImpl) drainClassLocked(
	key reschedulerKey,
	pq collection.Queue[rescheduledExecuable],
	pass *reschedulePass,
) {
	metrics.TaskReschedulerClassQueueDepth.With(r.metricsHandler).Record(int64(pq.Len()), r.classTags(key)...)

	for !pq.IsEmpty() {
		rescheduled := pq.Peek()
		if rescheduleTime := rescheduled.rescheduleTime; pass.now.Before(rescheduleTime) {
			pass.wakeAt(rescheduleTime)
			return
		}

		executable := rescheduled.executable
		if executable.State() == ctasks.TaskStateCancelled {
			pq.Remove()
			r.numExecutables--
			continue
		}

		metered := false
		if key.Throttle != (ThrottleKey{}) {
			allowed, admitted, retryAfter := r.throttleState.Admit(key.Throttle)
			if !allowed {
				pass.wakeAt(pass.now.Add(r.budgetRetryInterval(retryAfter)))
				return
			}
			metered = admitted
		}

		executable.SetScheduledTime(pass.now)
		if metered {
			// Mark before submitting: a worker can reach HandleErr before TrySubmit returns.
			executable.SetThrottleAdmitted(true)
		}
		if !r.scheduler.TrySubmit(executable) {
			if metered {
				executable.SetThrottleAdmitted(false)
				r.throttleState.Return(key.Throttle)
			}
			pass.wakeAt(pass.now.Add(
				backoff.Jitter(taskChanFullBackoff, taskChanFullBackoffJitterCoefficient)))
			return
		}

		pq.Remove()
		r.numExecutables--
	}
}

// The floor stops every shard polling at the bucket's refill rate, and the jitter stops them
// all polling on the same tick.
func (r *reschedulerImpl) budgetRetryInterval(eta time.Duration) time.Duration {
	const budgetRetryDivisor = 10

	window := r.throttleState.settings().Window
	interval := min(max(eta, window/budgetRetryDivisor), window)
	// Re-capped after jittering: a blocked class still has to look again within the window
	// that may have changed its rate.
	jittered := min(backoff.Jitter(interval, budgetRetryJitterCoefficient), window)
	return max(jittered, time.Millisecond)
}

func (r *reschedulerImpl) classTags(key reschedulerKey) []metrics.Tag {
	return []metrics.Tag{
		metrics.NamespaceIDTag(key.NamespaceID),
		metrics.TaskPriorityTag(key.Priority.String()),
		metrics.ResourceExhaustedCauseTag(key.Throttle.Cause),
	}
}

func (r *reschedulerImpl) cleanupPQ() {
	r.Lock()
	defer r.Unlock()

	for key, pq := range r.pqMap {
		if pq.IsEmpty() {
			delete(r.pqMap, key)
		}
	}
}

func (r *reschedulerImpl) drain() {
	r.Lock()
	defer r.Unlock()

	for key, pq := range r.pqMap {
		for !pq.IsEmpty() {
			pq.Remove()
		}
		delete(r.pqMap, key)
	}
	r.numExecutables = 0
}

func (r *reschedulerImpl) isStopped() bool {
	return atomic.LoadInt32(&r.status) == common.DaemonStatusStopped
}

func (r *reschedulerImpl) getOrCreateClassLocked(
	key reschedulerKey,
) collection.Queue[rescheduledExecuable] {
	if pq, ok := r.pqMap[key]; ok {
		return pq
	}

	pq := r.newPriorityQueue(nil)
	r.pqMap[key] = pq
	return pq
}

func (r *reschedulerImpl) newPriorityQueue(
	items []rescheduledExecuable,
) collection.Queue[rescheduledExecuable] {
	if items == nil {
		return collection.NewPriorityQueue(r.rescheduledExecuableCompareLess)
	}

	return collection.NewPriorityQueueWithItems(r.rescheduledExecuableCompareLess, items)
}

func (r *reschedulerImpl) rescheduledExecuableCompareLess(
	this rescheduledExecuable,
	that rescheduledExecuable,
) bool {
	return this.rescheduleTime.Before(that.rescheduleTime)
}
