package mapreduce.engine.priorityexecutor;

import java.util.concurrent.FutureTask;

import mapreduce.execution.task.Task;

public class ComparableTaskExecutionTask<T> extends FutureTask<T> implements Comparable<ComparableTaskExecutionTask<T>> {
	private volatile Task task;

	public ComparableTaskExecutionTask(Runnable runnable, T result, Task task) {
		super(runnable, result);
		this.task = task;
	}

	@Override
	public int compareTo(ComparableTaskExecutionTask<T> o) {
		if (task.activeCount() == o.task.activeCount()) {
			if (task.currentMaxNrOfSameResultHash() == o.task.currentMaxNrOfSameResultHash()) {
				return 0;
			} else {
				return task.currentMaxNrOfSameResultHash().compareTo(o.task.currentMaxNrOfSameResultHash());
			}
		} else {
			return task.activeCount().compareTo(o.task.activeCount());
		}
	}
}

// public ComparableFutureTask(Callable<T> callable, int priority) {
// super(callable);
// }