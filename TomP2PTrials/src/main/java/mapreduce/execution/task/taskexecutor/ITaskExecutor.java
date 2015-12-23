package mapreduce.execution.task.taskexecutor;

import java.util.List;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.context.IContext;
import mapreduce.utils.IAbortableExecution;

public interface ITaskExecutor extends IAbortableExecution {

	public void execute(final IMapReduceProcedure procedure, final Object key, final List<Object> values, final IContext context);

}
