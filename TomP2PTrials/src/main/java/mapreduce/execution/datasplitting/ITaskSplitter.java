package mapreduce.execution.datasplitting;

import mapreduce.execution.jobtask.Job;

public interface ITaskSplitter {

	/**
	 * Splits a job's data into appropriate portions (tasks), which are stored within the job
	 * 
	 * @param job
	 */
	public void split(final Job job);
}
