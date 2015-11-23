package mapreduce.execution.scheduling;

import java.util.List;

import mapreduce.execution.jobtask.Task;

public interface ITaskScheduler {
 
	/**
	 * 
	 * @param tasksToSchedule
	 * @return
	 */
	public Task schedule(List<Task> tasksToSchedule);

}
