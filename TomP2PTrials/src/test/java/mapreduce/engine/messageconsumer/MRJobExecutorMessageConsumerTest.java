package mapreduce.engine.messageconsumer;

import java.util.concurrent.PriorityBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.WordCountMapper;

public class MRJobExecutorMessageConsumerTest {

	private static JobCalculationMessageConsumer messageConsumer;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		JobCalculationExecutor jobExecutor = Mockito.mock(JobCalculationExecutor.class);

		messageConsumer = JobCalculationMessageConsumer.create(jobExecutor);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testHandleCompletedProcedure() {

	}
}
