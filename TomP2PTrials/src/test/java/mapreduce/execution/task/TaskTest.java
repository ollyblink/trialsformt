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

		Task task = Task.create("hello");
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
	public void testActiveCountAndNrOfSameResultHash() {
		Task task = Task.create("1").nrOfSameResultHash(2);
		assertEquals(true, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		task.incrementActiveCount();
		assertEquals(true, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.isFinished());

		task.incrementActiveCount();
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(2), task.activeCount());
		assertEquals(false, task.isFinished());

		// Cannot increment more than number of same result hash
		task.incrementActiveCount();
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(2), task.activeCount());
		assertEquals(false, task.isFinished());

		// Decrementing again
		task.decrementActiveCount();
		assertEquals(true, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.isFinished());

		task.decrementActiveCount();
		assertEquals(true, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		// Decrement cannot go below 0
		task.decrementActiveCount();
		assertEquals(true, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(false, task.isFinished());

		// Increment again by 1
		task.incrementActiveCount();
		assertEquals(true, task.canBeExecuted());
		assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.isFinished());

		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E1", 0, null).resultHash(Number160.ZERO));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.isFinished());

		// Not same result hash, currentMaxNr stays the same
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E1", 0, null).resultHash(Number160.ONE));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.isFinished());

		// Cannot be incremented more as there is already a result output hash
		task.incrementActiveCount();
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(false, task.isFinished());

		// same result hash, currentMaxNr stays increases
		task.addOutputDomain(ExecutorTaskDomain.create(task.key(), "E1", 0, null).resultHash(Number160.ONE));
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(1), task.activeCount());
		assertEquals(true, task.isFinished());

		// Decrementing again: Task stays finished and cannot be executed again
		task.decrementActiveCount();
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(true, task.isFinished());

		// Decrement not below 0
		task.decrementActiveCount();
		assertEquals(false, task.canBeExecuted());
		assertEquals(new Integer(2), task.currentMaxNrOfSameResultHash());
		assertEquals(new Integer(0), task.activeCount());
		assertEquals(true, task.isFinished());
	}

}
