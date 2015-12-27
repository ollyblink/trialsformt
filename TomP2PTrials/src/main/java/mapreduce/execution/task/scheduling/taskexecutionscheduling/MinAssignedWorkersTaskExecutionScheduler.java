package mapreduce.execution.task.scheduling.taskexecutionscheduling;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import mapreduce.execution.task.Task;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.sortingcomparators.MinAssignedWorkerTaskExecutionSortingComparator;

/**
 * Scheduler simply orders the tasks such that those tasks with least currently assigned peers to execute it are favoured. Tasks with less different
 * executors are favoured over such that already have a larger diversity. Lastly, Tasks with more <code>JobStatus.EXECUTING_TASK</code> are also
 * favoured over tasks with more <code>JobStatus.FINISHED_TASK</code>. If all tasks are finished, null is returned.
 * 
 * @author ozihler
 *
 */
public class MinAssignedWorkersTaskExecutionScheduler extends AbstractTaskExecutionScheduler {
	// private static Logger logger = LoggerFactory.getLogger(MinAssignedWorkersTaskScheduler.class);
	// private boolean isFirstTaskRandom;
	private MinAssignedWorkerTaskExecutionSortingComparator comparator;
	private RandomTaskExecutionScheduler randomTaskScheduler;

	private MinAssignedWorkersTaskExecutionScheduler() {
		this.comparator = MinAssignedWorkerTaskExecutionSortingComparator.newInstance();
	}

	public static MinAssignedWorkersTaskExecutionScheduler newInstance() {
		return new MinAssignedWorkersTaskExecutionScheduler().randomizeFirstTask(true);
	}

	public MinAssignedWorkersTaskExecutionScheduler randomizeFirstTask(boolean isFirstTaskRandom) {
		this.randomTaskScheduler = (isFirstTaskRandom ? RandomTaskExecutionScheduler.newInstance() : null);
		return this;
	}

	@Override
	protected Task scheduleNonNull(List<Task> tasksToSchedule) {
		Task assignedTask = null;
		if (!allTasksAreFinished(tasksToSchedule)) {
			if (randomTaskScheduler != null && noTaskAssignedYet(tasksToSchedule)) {
				assignedTask = randomTaskScheduler.scheduleNonNull(tasksToSchedule);
			} else {
				Collections.sort(tasksToSchedule, this.comparator);
				Task task = tasksToSchedule.get(0);
				if (!task.isFinished() || task.isActive()) {
					assignedTask = task;
				}
			}
		} else {
			// all tasks finished... set procedure to be finished
			this.procedureInformation.isFinished(true);
		}
		return assignedTask;
	}

}
