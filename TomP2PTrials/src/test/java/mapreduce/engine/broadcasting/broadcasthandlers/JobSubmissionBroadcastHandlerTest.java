package mapreduce.engine.broadcasting.broadcasthandlers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Random;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileUtils;

public class JobSubmissionBroadcastHandlerTest {
	private static Random random = new Random();
	private static JobSubmissionBroadcastHandler broadcastHandler;
	private static Job job;
	private static IMessageConsumer messageConsumer;

	@BeforeClass
	public static void setUp() {

		String jsMapper = FileUtils.INSTANCE
				.readLines(System.getProperty("user.dir") + "/src/main/java/mapreduce/execution/procedures/wordcountmapper.js");
		String jsReducer = FileUtils.INSTANCE
				.readLines(System.getProperty("user.dir") + "/src/main/java/mapreduce/execution/procedures/wordcountreducer.js");

		job = Job.create("Submitter").addSucceedingProcedure(jsMapper, jsReducer, 1, 1, false, false).addSucceedingProcedure(jsReducer, null, 1, 1,
				false, false);

		messageConsumer = Mockito.mock(IMessageConsumer.class);
		IExecutor executor = Mockito.mock(IExecutor.class);
		Mockito.when(executor.id()).thenReturn("Executor");
		Mockito.when(messageConsumer.executor()).thenReturn(executor);

		broadcastHandler = JobSubmissionBroadcastHandler.create(1);
		broadcastHandler.messageConsumer(messageConsumer);
	}

	@Test
	public void test() {
		fail();
	}

}
