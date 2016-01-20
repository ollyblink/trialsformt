package mapreduce.execution.task;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.tasks.Task;
import net.tomp2p.peers.Number160;

public class TaskTest {

	@Test
	public void testTask() {
		String submitter = "S1";
		String executor1 = "E1";
		String executor2 = "E2";
		int trueResult = 100;
		int falseResult = 99;

		Task task = Task.create("hello", "E1");
		ExecutorTaskDomain etd = ExecutorTaskDomain
				.create("hello", executor1, task.newStatusIndex(), JobProcedureDomain.create("job1", 0, submitter, "WordCount", 0))
				.resultHash(Number160.createHash(trueResult));
		task.addOutputDomain(etd);

		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());

		task.nrOfSameResultHash(2);

		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());

		ExecutorTaskDomain etd2 = ExecutorTaskDomain
				.create("hello", executor1, task.newStatusIndex(), JobProcedureDomain.create("job1", 0, submitter, "WordCount", 0))
				.resultHash(Number160.createHash(trueResult));

		task.addOutputDomain(etd2);
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());

		etd2.resultHash(Number160.createHash(falseResult));
		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());

		etd2.resultHash(Number160.createHash(trueResult));
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());

		task.nrOfSameResultHash(3);

		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());

		ExecutorTaskDomain etd3 = ExecutorTaskDomain
				.create("hello", executor2, task.newStatusIndex(), JobProcedureDomain.create("job1", 0, submitter, "WordCount", 0))
				.resultHash(Number160.createHash(trueResult));
		task.addOutputDomain(etd3);
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain()); // Always take the first one...
	}

	@Test
	public void testActiveCountAndNrOfSameResultHashSameExecutorPossible() {
		// ===========================================================================================================================================
		// Here the task may be executed by the same executor multiple times.
		// ===========================================================================================================================================

		String localExecutorId = "E1";
		Task task = (Task) Task.create("1", localExecutorId).nrOfSameResultHash(2).needsMultipleDifferentExecutors(false);
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(true, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		task.incrementActiveCount();
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(true, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		task.incrementActiveCount();
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(2), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Cannot increment more than number of same result hash
		task.incrementActiveCount();
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(2), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Decrementing again
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), localExecutorId, 0, null).resultHash(Number160.ZERO));
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Not same result hash, currentMaxNr stays the same
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), localExecutorId, 1, null).resultHash(Number160.ONE));
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(true, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// Increment again by 1
		task.incrementActiveCount();
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// same domain cannot be added twice. Stays the same!
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), localExecutorId, 0, null).resultHash(Number160.ONE));
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(false, task.isFinished());

		// same result hash, currentMaxNr increases, different executor
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E2", 0, null).resultHash(Number160.ONE));
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());
		
		// Another of the other executor... ignored
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E2", 1, null).resultHash(Number160.ONE));
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());
		
		// Another of the other executor... ignored
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E2", 2, null).resultHash(Number160.ONE));
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());

		// Current executor finishes, but has no effect anymore except decreasing the active count
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), localExecutorId, 2, null).resultHash(Number160.ZERO));
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.canBeExecuted());
		assertEquals(true, task.isFinished());
	}

	@Test
	public void testActiveCountAndNrOfSameResultHashDifferentExecutors() {
		// ===========================================================================================================================================
		// Here the task may be executed by the same executor ONLY ONCE.
		// ===========================================================================================================================================

		String executor = "E1";
		Task task = (Task) Task.create("1", executor).nrOfSameResultHash(2).needsMultipleDifferentExecutors(true);
		assertEquals(true, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		task.incrementActiveCount();
		// Now it should not be possible to execute the task once more
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.isFinished());

		// Now this one finishes the execution and one result domain for this executor is available --> active count cannot be increased anymore, task
		// may not be executed anymore
		task.addOutputDomain(ExecutorTaskDomain.create("1", executor, -1, null));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());
	}

}
