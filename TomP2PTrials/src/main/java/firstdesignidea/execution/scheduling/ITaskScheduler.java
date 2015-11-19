package firstdesignidea.execution.scheduling;

import java.util.List;

import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;

public interface ITaskScheduler {
 
	/** 
	 * schedules the tasks for a job to execute out according to specified algorithm 
	 * @param job job to schedule tasks from
	 * @return ordered list of tasks
	 */
	public List<Task> schedule(Job job);

}
