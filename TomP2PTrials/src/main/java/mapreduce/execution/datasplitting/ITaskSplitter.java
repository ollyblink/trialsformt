package mapreduce.execution.datasplitting;

import mapreduce.execution.jobtask.Job;
import mapreduce.storage.DHTConnectionProvider;

public interface ITaskSplitter {

	public void splitAndEmit(final Job job, DHTConnectionProvider dhtConnectionProvider);

	/**
	 * Splits a job's data into appropriate portions (tasks), which are stored within the job
	 * @param job 
	 */
	public void split(final Job job);
 }
