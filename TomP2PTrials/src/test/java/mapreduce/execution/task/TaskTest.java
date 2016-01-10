package mapreduce.execution.task;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
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
				.create("hello", executor1, task.newStatusIndex(), JobProcedureDomain.create("job1", submitter, "WordCount", 0))
				.resultHash(Number160.createHash(trueResult));
 		task.addOutputDomain(etd);

		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain());

		task.nrOfSameResultHash(2);

		assertEquals(false, task.isFinished());
		assertEquals(null, task.resultOutputDomain());

 		ExecutorTaskDomain etd2 = ExecutorTaskDomain
				.create("hello", executor1, task.newStatusIndex(), JobProcedureDomain.create("job1", submitter, "WordCount", 0))
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
				.create("hello", executor2, task.newStatusIndex(), JobProcedureDomain.create("job1", submitter, "WordCount", 0))
				.resultHash(Number160.createHash(trueResult));
 		task.addOutputDomain(etd3);
		assertEquals(true, task.isFinished());
		assertEquals(etd, task.resultOutputDomain()); //Always take the first one...
	}

}
