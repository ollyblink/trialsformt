package mapreduce.execution.datasplitting;

import com.google.common.collect.Multimap;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;

public interface ITaskSplitter {

	/**
	 * Splits a job's data into appropriate portions (tasks), which are stored within the job
	 * 
	 * @param job
	 */
	public void split(final Job job);

	public Multimap<Task, Comparable> keysForEachTask();
}
