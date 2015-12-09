package mapreduce.execution.task.taskexecutorscleaner;

import com.google.common.collect.Multimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.storage.LocationBean;

public interface IJobCleaner {

	/**
	 * 
	 * @param job to clean
	 * @return tuples data to remove
	 */
	public Multimap<Task, LocationBean> cleanUp(final Job job);

}
