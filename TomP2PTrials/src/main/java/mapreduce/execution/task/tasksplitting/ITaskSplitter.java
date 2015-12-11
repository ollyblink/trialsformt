package mapreduce.execution.task.tasksplitting;

import com.google.common.collect.Multimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;

public interface ITaskSplitter {

	/**
	 * Splits a job's data into appropriate portions (tasks), which are stored within the job
	 * 
	 * @param job
	 */
	public void split(String inputPath, long maxFileSize);

}
