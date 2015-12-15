package mapreduce.execution.task.taskdatacomposing;

public interface ITaskDataComposer {
	/**
	 * Works as follows: data is added until the specified max file size is reached. then the method returns the final data string. in any other case,
	 * the method returns null
	 * 
	 * @param value
	 * @return
	 */
	public String append(String value);

	public String fileEncoding();
}
