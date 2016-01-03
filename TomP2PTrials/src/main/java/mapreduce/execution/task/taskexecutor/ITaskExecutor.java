package mapreduce.execution.task.taskexecutor;

import java.util.List;

import mapreduce.execution.context.IContext;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.utils.IAbortableExecution;

public interface ITaskExecutor extends IAbortableExecution {

	public void execute(final IExecutable procedure, final Object key, final List<Object> values, final IContext context);

}
