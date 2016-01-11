package mapreduce.engine.priorityexecutor;

import org.junit.Test;

import mapreduce.execution.task.Task;

public class ComparableTaskExecutionTaskTest {

	@Test
	public void test() {
		Task task = Task.create("T1");
		Object runnable;
		ComparableTaskExecutionTask<Object> t = new ComparableTaskExecutionTask<>(runnable, null, task);
	}

}
