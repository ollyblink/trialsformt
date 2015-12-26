package mapreduce.execution.task.taskdatacomposing;

import mapreduce.utils.FileSize;

public interface ITaskDataComposer {
	/**
	 * Works as follows: data is added until the specified max file size is reached. then the method returns the final data string. in any other case,
	 * the method returns null
	 * 
	 * @param value
	 * @return
	 */
	public String append(String value);
	
	/**
	 * Returns, without clearing, all values currently appended
	 * @return
	 */
	public String currentValues();

	public String fileEncoding();

	public ITaskDataComposer fileEncoding(String fileEncoding);

	public ITaskDataComposer maxFileSize(FileSize maxFileSize);

	public ITaskDataComposer splitValue(String splitValue);

	public void reset();
}