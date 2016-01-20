package mapreduce.engine.priorityexecutor;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import mapreduce.engine.multithreading.PriorityExecutor;
import mapreduce.execution.tasks.Task;

public class PriorityExecutorTest {

	@Test
	public void testTaskSubmission() {
		// ===========================================================================================================================================
		// This test should show if cancellation of Futures works (see JobCalculationMessageConsumer for the actual methods that cancel procedures and
		// tasks and JobCalculationMessageConsumerTest for the tests of the corresponding methods). This is a continuation of the tests there.
		// ===========================================================================================================================================
		PriorityExecutor executor = PriorityExecutor.newFixedThreadPool(1);
		List<Task> tasks = new ArrayList<>();
		tasks.add(Task.create("hello", "E1").nrOfSameResultHash(3));
		tasks.add(Task.create("world", "E1").nrOfSameResultHash(3));
		tasks.add(Task.create("this", "E1").nrOfSameResultHash(3));
		tasks.add(Task.create("is", "E1").nrOfSameResultHash(3));
		tasks.add(Task.create("a", "E1").nrOfSameResultHash(3));
		tasks.add(Task.create("test", "E1").nrOfSameResultHash(3));

		// Submitting
		for (Task task : tasks) {
			executor.submit(new Runnable() {

				@Override
				public void run() {
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}, task);
		}

		// ===========================================================================================================================================
		// The test expects that only the first task is executed and, as execution takes 10 seconds, other tasks in the queue are aborted and,
		// therefore, not executed anymore if they correspond to the specified procedure or task. Each task should occur 3 times in the queue because
		// it needs 3 same result hashs to be completed
		// ===========================================================================================================================================
		// Test task abortion
	}

	@Test
	public void testMessageSubmission() {

		fail();
	}
}
