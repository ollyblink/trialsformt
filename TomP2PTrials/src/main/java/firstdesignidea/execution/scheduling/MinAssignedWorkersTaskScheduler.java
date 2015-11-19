package firstdesignidea.execution.scheduling;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.JobStatus;
import firstdesignidea.execution.jobtask.Task;

/**
 * Scheduler simply orders the tasks such that those tasks with least currently assigned peers to execute it are favoured. Tasks with more
 * <code>JobStatus.EXECUTING_TASK</code> are also favoured over tasks with more <code>JobStatus.FINISHED_TASK</code>
 * 
 * @author ozihler
 *
 */
public class MinAssignedWorkersTaskScheduler implements ITaskScheduler {

	@Override
	public List<Task> schedule(Job job) {
		List<Task> tasks = job.tasks();
		Collections.sort(tasks, new Comparator<Task>() {

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
		return tasks;
	}

}
