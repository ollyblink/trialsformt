package mapreduce.execution.task.taskexecutor;

import com.google.common.collect.Multimap;

import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.task.Task;
import mapreduce.utils.IAbortableExecution;

public interface ITaskExecutor extends IAbortableExecution{

	public void executeTask(final Task task, final IContext context, final Multimap<Object, Object> dataForTask);



}
