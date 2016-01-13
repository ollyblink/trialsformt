package mapreduce.engine.broadcasting;

import static org.junit.Assert.*;

import java.util.Random;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.executor.MRJobSubmissionManager;
import mapreduce.engine.messageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;

public class MRBroadcastHandlerTest {
	private static Random random = new Random();
	private static MRBroadcastHandler broadcastHandler;
	private static IDHTConnectionProvider dhtConnectionProvider;
	private static Job job;
	private static MRJobExecutionManagerMessageConsumer messageConsumer;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		messageConsumer = MRJobExecutionManagerMessageConsumer.create();
		dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 1, messageConsumer);
		messageConsumer.dhtConnectionProvider(dhtConnectionProvider);
		job = Job.create("Submitter").addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);
		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
		broadcastHandler = dhtConnectionProvider.broadcastHandler();
	}

	@Test
	public void test() throws Exception {
		IBCMessage msg = CompletedBCMessage.createCompletedProcedureBCMessage(JobProcedureDomain.create(job.id(), "Submitter", "INITIAL", -1),
				JobProcedureDomain.create(job.id(), "Submitter", StartProcedure.class.getSimpleName(), 0));

		assertEquals(true, broadcastHandler.jobFutures().isEmpty());
		broadcastHandler.addExternallyReceivedMessage(msg);
		assertEquals(false, broadcastHandler.jobFutures().isEmpty());
		assertEquals(true, broadcastHandler.jobFutures().keySet().contains(job));
		ListMultimap<Job, Future<?>> jobFutures = broadcastHandler.jobFutures();
		for (Future<?> f : jobFutures.values()) {
			assertTrue(f.isDone());
			System.err.println(f.get());
		}
	}

	@Test
	public void execute() {
		MRJobSubmissionManager submitter = MRJobSubmissionManager.create(dhtConnectionProvider);
		dhtConnectionProvider.broadcastHandler().messageConsumer(messageConsumer);
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/testFiles";
		Job job = Job.create(submitter.id(), PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES).fileInputFolderPath(fileInputFolderPath)

				.addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
				// .addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false)
				;

		submitter.submit(job);
		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
