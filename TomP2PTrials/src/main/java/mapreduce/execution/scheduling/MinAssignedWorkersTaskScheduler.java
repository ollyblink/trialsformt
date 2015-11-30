package mapreduce.execution.scheduling;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import mapreduce.execution.jobtask.Task;

/**
 * Scheduler simply orders the tasks such that those tasks with least currently assigned peers to execute it are favoured. Tasks with less different
 * executors are favoured over such that already have a larger diversity. Lastly, Tasks with more <code>JobStatus.EXECUTING_TASK</code> are also
 * favoured over tasks with more <code>JobStatus.FINISHED_TASK</code>. If all tasks are finished, null is returned.
 * 
 * @author ozihler
 *
 */
public class MinAssignedWorkersTaskScheduler extends AbstractTaskScheduler {
//	private static Logger logger = LoggerFactory.getLogger(MinAssignedWorkersTaskScheduler.class);
	private boolean isFirstTaskRandom;
	private Comparator<? super Task> comparator;
	private RandomTaskScheduler randomTaskScheduler;

	private class TaskComparator implements Comparator<Task> {

		@Override
		public int compare(Task t1, Task t2) {
			int t1Finished = t1.totalNumberOfFinishedExecutions();
			int t2Finished = t2.totalNumberOfFinishedExecutions();
			if (t1Finished > t2Finished) {
				return 1;
			} else if (t1Finished < t2Finished) {
				return -1;
			} else {
				int t1differentFinished = t1.numberOfDifferentPeersExecutingTask();
				int t2differentFinished = t2.numberOfDifferentPeersExecutingTask();
				if (t1differentFinished > t2differentFinished) {
					return 1;
				} else if (t1differentFinished < t2differentFinished) {
					return -1;
				} else {
					int t1Executing = t1.totalNumberOfCurrentExecutions();
					int t2Executing = t2.totalNumberOfCurrentExecutions();
					if (t1Executing > t2Executing) {
						return 1;
					} else if (t1Executing < t2Executing) {
						return -1;
					} else {
						return 0;
					}
				}
			}
		}
	}

	private MinAssignedWorkersTaskScheduler() {
		this.comparator = new MinAssignedWorkersTaskScheduler.TaskComparator();
	}

	public static MinAssignedWorkersTaskScheduler newInstance() {
		return new MinAssignedWorkersTaskScheduler().randomizeFirstTask(true);
	}

	public MinAssignedWorkersTaskScheduler randomizeFirstTask(boolean isFirstTaskRandom) {
		this.isFirstTaskRandom = isFirstTaskRandom;
		if (this.isFirstTaskRandom) {
			this.randomTaskScheduler = RandomTaskScheduler.newRandomTaskScheduler();
		} else {
			this.randomTaskScheduler = null;
		}
		return this;
	}

	@Override
	public Task schedule(List<Task> tasksToSchedule) {
		Task assignedTask = null;
		if (tasksToSchedule != null && tasksToSchedule.size() > 0) {
			if (!allTasksAreFinished(tasksToSchedule)) {
				if (isFirstTaskRandom && NoTaskAssignedYet(tasksToSchedule)) {
					assignedTask = randomTaskScheduler.schedule(tasksToSchedule);
				} else {
					Collections.sort(tasksToSchedule, this.comparator);
					if (!tasksToSchedule.get(0).isFinished()) {
						assignedTask = tasksToSchedule.get(0);
					}
				}
			}
		}
		return assignedTask;
	}

}
