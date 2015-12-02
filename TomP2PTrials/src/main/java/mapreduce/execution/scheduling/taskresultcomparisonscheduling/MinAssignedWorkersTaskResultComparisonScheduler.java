package mapreduce.execution.scheduling.taskresultcomparisonscheduling;

import java.util.Collections;
import java.util.List;

import mapreduce.execution.jobtask.Task;
import mapreduce.execution.scheduling.ITaskScheduler;
import mapreduce.execution.scheduling.taskresultcomparisonscheduling.sortingcomparators.MinAssignedWorkerTaskResultSortingComparator;

/**
 * Scheduler simply orders the tasks such that those tasks with least currently assigned peers to execute it are favoured. Tasks with less different
 * executors are favoured over such that already have a larger diversity. Lastly, Tasks with more <code>JobStatus.EXECUTING_TASK</code> are also
 * favoured over tasks with more <code>JobStatus.FINISHED_TASK</code>. If all tasks are finished, null is returned.
 * 
 * @author ozihler
 *
 */
public class MinAssignedWorkersTaskResultComparisonScheduler extends AbstractTaskResultComparisonScheduler {
	// private static Logger logger = LoggerFactory.getLogger(MinAssignedWorkersTaskScheduler.class);
	// private boolean isFirstTaskRandom;
	private MinAssignedWorkerTaskResultSortingComparator comparator;
	private RandomTaskResultComparisonScheduler randomTaskResultComparisonScheduler;

	private MinAssignedWorkersTaskResultComparisonScheduler() {
		this.comparator = MinAssignedWorkerTaskResultSortingComparator.newInstance();
	}

	public static MinAssignedWorkersTaskResultComparisonScheduler newInstance() {
		return new MinAssignedWorkersTaskResultComparisonScheduler().randomizeFirstTask(true);
	}

	private MinAssignedWorkersTaskResultComparisonScheduler randomizeFirstTask(boolean randomizeFirstTask) {
		this.randomTaskResultComparisonScheduler = (randomizeFirstTask ? RandomTaskResultComparisonScheduler.newInstance() : null);
		return this;
	}

	

	@Override
	protected Task scheduleNonNull(List<Task> tasksToSchedule) {
		Task assignedTask = null;
		if (!allTasksResultsAreAssigned(tasksToSchedule)) {
			if (noTaskResultAssignedYet(tasksToSchedule) && randomTaskResultComparisonScheduler != null) {
				assignedTask = randomTaskResultComparisonScheduler.schedule(tasksToSchedule);
			} else {
				Collections.sort(tasksToSchedule, this.comparator);
				if (tasksToSchedule.get(0).finalDataLocation() != null) {
					assignedTask = tasksToSchedule.get(0);
				}
			}
		}
		return assignedTask;
	}

}
