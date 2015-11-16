package firstdesignidea.execution.scheduling;

import java.util.List;

import firstdesignidea.execution.jobtask.Job;

public interface IJobScheduler {
 
	/** schedules the next job to execute out of a list of jobs according to specified algorithm*/
	public Job schedule(List<Job> jobs);

}
