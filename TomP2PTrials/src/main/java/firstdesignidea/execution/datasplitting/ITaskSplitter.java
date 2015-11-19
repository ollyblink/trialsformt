package firstdesignidea.execution.datasplitting;

import java.util.List;

import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.storage.DHTConnectionProvider;

public interface ITaskSplitter {

	public void splitAndEmit(final Job job, DHTConnectionProvider dhtConnectionProvider);

	/**
	 * Splits a job's data into appropriate portions (tasks), which are stored within the job
	 * @param job 
	 */
	public void split(final Job job);
	public void emit(final Job job, DHTConnectionProvider dhtConnectionProvider);
}
