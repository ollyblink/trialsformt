package mapreduce.execution.scheduling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.jobtask.Task;

/**
 * Scheduler simply orders the tasks such that those tasks with least currently assigned peers to execute it are favoured. Tasks with more
 * <code>JobStatus.EXECUTING_TASK</code> are also favoured over tasks with more <code>JobStatus.FINISHED_TASK</code>
 * 
 * @author ozihler
 *
 */
public class MinAssignedWorkersTaskScheduler implements ITaskScheduler {
	private static Logger logger = LoggerFactory.getLogger(MinAssignedWorkersTaskScheduler.class);

	private MinAssignedWorkersTaskScheduler() {
		// TODO Auto-generated constructor stub
	}

	public static MinAssignedWorkersTaskScheduler newRandomTaskScheduler() {
		return new MinAssignedWorkersTaskScheduler();
	}

	@Override
	public Task schedule(List<Task> tasksToSchedule) {
 
		Collections.sort(tasksToSchedule, new Comparator<Task>() {

			@Override
			public int compare(Task t1, Task t2) {

				int t1Finished = t1.numberOfPeersWithStatus(JobStatus.FINISHED_TASK);
				int t2Finished = t2.numberOfPeersWithStatus(JobStatus.FINISHED_TASK);
				if (t1Finished > t2Finished) {
					return 1;
				} else if (t1Finished < t2Finished) {
					return -1;
				} else {
					int t1Executing = t1.numberOfPeersWithStatus(JobStatus.EXECUTING_TASK);
					int t2Executing = t2.numberOfPeersWithStatus(JobStatus.EXECUTING_TASK);
					if (t1Executing > t2Executing) {
						return 1;
					} else if (t1Executing < t2Executing) {
						return -1;
					} else {
						if (t1.numberOfAssignedPeers() > t2.numberOfAssignedPeers()) {
							return 1;
						} else if (t1.numberOfAssignedPeers() < t2.numberOfAssignedPeers()) {
							return -1;
						} else {
							return 0;
						}
					}
				}
			}
		});

		String taskAssignments = "";
		for (Task task : tasksToSchedule) {
			taskAssignments += JobStatus.EXECUTING_TASK + ": " + task.numberOfPeersWithStatus(JobStatus.EXECUTING_TASK) + "; ";
			taskAssignments += JobStatus.FINISHED_TASK + ": " + task.numberOfPeersWithStatus(JobStatus.FINISHED_TASK)+"\n";
		}
		logger.warn("MinAssignedWorkersTaskScheduler::schedule(): Task assignments:" + taskAssignments);

		if (tasksToSchedule == null || tasksToSchedule.size() == 0) {
			return null;
		} else {
			return tasksToSchedule.get(0);
		}
	}

}
