package mapreduce.engine.messageconsumer;

import java.util.concurrent.PriorityBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.engine.messageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.WordCountMapper;

public class MRJobExecutorMessageConsumerTest {

	private static MRJobExecutionManagerMessageConsumer messageConsumer;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		MRJobExecutionManager jobExecutor = Mockito.mock(MRJobExecutionManager.class);

		messageConsumer = MRJobExecutionManagerMessageConsumer.create(jobExecutor);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testHandleCompletedProcedure() {

	}
}
