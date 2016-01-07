package mapreduce.engine.messageconsumer;

import java.util.SortedMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.broadcasting.IBCMessage;
import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.engine.messageConsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.procedures.WordCountMapper;

public class MRJobExecutorMessageConsumerTest {

	private static final String[] TEST_KEYS = { "hello", "world", "this", "is", "a", "test" };
	private static MRJobExecutionManagerMessageConsumer testMessageConsumer;
	private static String peer1;
	private static String peer2;
	private static String peer3;
	private static Job job;
	private static MRJobExecutionManagerMessageConsumer messageConsumer;
	private static SortedMap<Job, PriorityBlockingQueue<IBCMessage>> jobs;
	private static JobProcedureDomain inputDomain;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		peer1 = "EXECUTOR_1";
		peer2 = "EXECUTOR_2";
		peer3 = "EXECUTOR_3";

		MRJobExecutionManager jobExecutor = Mockito.mock(MRJobExecutionManager.class);
		messageConsumer = MRJobExecutionManagerMessageConsumer.create(jobExecutor);
		jobs = messageConsumer.jobs();

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testHandleCompletedTask() {
		// messageConsumer.handleCompletedTask(outputDomain, inputDomain, tasksSize);
	}

	@Test
	public void testHandleCompletedProcedure() {
		job = Job.create("TEST", PriorityLevel.MODERATE).addSucceedingProcedure(WordCountMapper.create());
		inputDomain = JobProcedureDomain.create(job.id(), peer1, job.currentProcedure().executable().getClass().getSimpleName(),
				job.currentProcedure().procedureIndex());
		job.incrementProcedureIndex();
		job.currentProcedure().inputDomain(inputDomain);
		job.currentProcedure().addOutputDomain(JobProcedureDomain.create(job.id(), peer1,
				job.currentProcedure().executable().getClass().getSimpleName(), job.currentProcedure().procedureIndex()));

		messageConsumer.handleCompletedProcedure(outputDomain, inputDomain, tasksSize);
	}
}
