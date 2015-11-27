package mapreduce.execution.scheduling;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.jobtask.Task;

/**
 * Scheduler simply orders the tasks such that those tasks with least currently assigned peers to execute it are favoured. Tasks with less different
 * executors are favoured over such that already have a larger diversity. Lastly, Tasks with more <code>JobStatus.EXECUTING_TASK</code> are also
 * favoured over tasks with more <code>JobStatus.FINISHED_TASK</code>. If all tasks are finished, null is returned.
 * 
 * @author ozihler
 *
 */
public class MinAssignedWorkersTaskScheduler implements ITaskScheduler {
	private static Logger logger = LoggerFactory.getLogger(MinAssignedWorkersTaskScheduler.class);

	private MinAssignedWorkersTaskScheduler() {
		// TODO Auto-generated constructor stub
	}

	public static MinAssignedWorkersTaskScheduler newMinAssignedWorkersTaskScheduler() {
		return new MinAssignedWorkersTaskScheduler();
	}

	@Override
	public Task schedule(List<Task> tasksToSchedule) { 
		if (tasksToSchedule == null || tasksToSchedule.size() == 0) {
			return null;
		} else {
			boolean allFinished = true;
			for (Task task : tasksToSchedule) {
				System.err.println("task.totalNumberOfFinishedExecutions() >= task.maxNrOfFinishedWorkers()"+task.totalNumberOfFinishedExecutions() +" >= "+task.maxNrOfFinishedWorkers());

				if (task.totalNumberOfFinishedExecutions() >= task.maxNrOfFinishedWorkers()) {
					task.isFinished(true);
				}
				if (!task.isFinished()) {
					allFinished = false;
					break;
				}
			}
			if (allFinished) {
				return null;
			} else {
				Collections.sort(tasksToSchedule, new Comparator<Task>() {

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
				});

				if (tasksToSchedule.get(0).isFinished()) {
					return null;
				} else {
					return tasksToSchedule.get(0);
				}
			}
		}
	}

}
