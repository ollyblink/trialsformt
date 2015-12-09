package mapreduce.execution.task.taskexecutor;

import com.google.common.collect.Multimap;

import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.task.Task;

public interface ITaskExecutor {

	public void executeTask(final Task task, final IContext context, final Multimap<Object, Object> dataForTask);

	public void abortTaskExecution();

	public void abortedTaskExecution(boolean abortedTaskExecution);

	public boolean abortedTaskExecution();


}
