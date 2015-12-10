package mapreduce.utils;

public interface IAbortableExecution {

	public void abortTaskExecution();

	public boolean abortedTaskExecution();
}
