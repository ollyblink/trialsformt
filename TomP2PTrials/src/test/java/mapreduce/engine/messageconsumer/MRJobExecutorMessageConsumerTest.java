package mapreduce.engine.messageconsumer;

import java.util.concurrent.PriorityBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.engine.messageConsumer.MRJobExecutionManagerMessageConsumer;
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

	// @Test
	// public void testHandleCompletedTask() {
	// // messageConsumer.handleCompletedTask(outputDomain, inputDomain, tasksSize);
	// }

	@Test
	public void testHandleCompletedProcedure() {

		Job originalJob = Job.create("TEST").addSucceedingProcedure(WordCountMapper.create(), null, 1, 1);
		messageConsumer.jobs().put(originalJob, new PriorityBlockingQueue<>());

		String procedureExecutor = "E1";
		JobProcedureDomain inputDomain = JobProcedureDomain.create(originalJob.id(), procedureExecutor,
				originalJob.currentProcedure().executable().getClass().getSimpleName(), originalJob.currentProcedure().procedureIndex());

		originalJob.incrementProcedureIndex();
		originalJob.currentProcedure().inputDomain(inputDomain);
		originalJob.currentProcedure().addOutputDomain(JobProcedureDomain.create(originalJob.id(), procedureExecutor,
				originalJob.currentProcedure().executable().getClass().getSimpleName(), originalJob.currentProcedure().procedureIndex()));
		Job job = originalJob.clone();
		JobProcedureDomain resultOutputDomain = (JobProcedureDomain) job.currentProcedure().resultOutputDomain();
		JobProcedureDomain inputDomain2 = job.currentProcedure().inputDomain();
		
		messageConsumer.handleCompletedProcedure(job, resultOutputDomain, inputDomain2);
	}
}
