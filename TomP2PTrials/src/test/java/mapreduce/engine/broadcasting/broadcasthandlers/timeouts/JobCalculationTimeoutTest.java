package mapreduce.engine.broadcasting.broadcasthandlers.timeouts;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.timeout.JobCalculationTimeout;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.storage.IDHTConnectionProvider;

public class JobCalculationTimeoutTest {

	private static List tasks;
	private static JobProcedureDomain oldID;
	private static Procedure procedure;
	private static Job job;
	private static JobCalculationExecutor executor;
	private static JobCalculationMessageConsumer msgConsumer;
	private static JobCalculationBroadcastHandler broadcastHandler;
	private static IDHTConnectionProvider mockDHT;
	private static JobProcedureDomain inputDomain;
	private static CompletedBCMessage bcMessage;
	private static long currentTimestamp;
	private static long timeToLive;
	private static CompletedBCMessage mockMsg;

	@BeforeClass
	public static void before() {
		  currentTimestamp = System.currentTimeMillis();

		  timeToLive = 2000l;

		// Tasks
		tasks = Mockito.mock(List.class);
		Mockito.when(tasks.size()).thenReturn(1);
		// Procedure
		oldID = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(oldID.procedureIndex()).thenReturn(-1);
		Mockito.when(oldID.expectedNrOfFiles()).thenReturn(1);

		procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.tasks()).thenReturn(tasks);
		Mockito.when(procedure.dataInputDomain()).thenReturn(oldID);
		// Job
		job = Mockito.mock(Job.class);
		Mockito.when(job.id()).thenReturn("J1");
		Mockito.when(job.currentProcedure()).thenReturn(procedure);
		// Executor
		executor = Mockito.mock(JobCalculationExecutor.class);
		Mockito.when(executor.id()).thenReturn("E1");
		mockMsg = Mockito.mock(CompletedBCMessage.class);
		Mockito.when(executor.tryFinishProcedure(procedure)).thenReturn(mockMsg);

		// MessageConsumer
		msgConsumer = Mockito.mock(JobCalculationMessageConsumer.class);
		Mockito.when(msgConsumer.executor()).thenReturn(executor);
		// BCHandler
		broadcastHandler = Mockito.mock(JobCalculationBroadcastHandler.class);
		Mockito.when(broadcastHandler.executorId()).thenReturn("E1");
		Mockito.when(broadcastHandler.messageConsumer()).thenReturn(msgConsumer);
		Mockito.when(broadcastHandler.getJob(job.id())).thenReturn(job);
		mockDHT = Mockito.mock(IDHTConnectionProvider.class);
		Mockito.when(broadcastHandler.dhtConnectionProvider()).thenReturn(mockDHT);
		// Input domain
		  inputDomain = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(inputDomain.procedureIndex()).thenReturn(-1);
		Mockito.when(inputDomain.expectedNrOfFiles()).thenReturn(2);
		// BCMessage
		  bcMessage = Mockito.mock(CompletedBCMessage.class);
		Mockito.when(bcMessage.inputDomain()).thenReturn(inputDomain);
	}

	@Test
	public void testUpdatingExpectedNrOfTasks() throws InterruptedException {

		// Actual timeout
		JobCalculationTimeout timeout = new JobCalculationTimeout(broadcastHandler, job, currentTimestamp, bcMessage, timeToLive);
		new Thread(timeout).start();
		Thread.sleep(1000);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		Thread.sleep(3000);
		Mockito.verify(job, Mockito.times(1)).currentProcedure();
		Mockito.verify(procedure, Mockito.times(1)).tasks();
		Mockito.verify(procedure, Mockito.times(1)).dataInputDomain();
		Mockito.verify(tasks, Mockito.times(1)).size();
		Mockito.verify(broadcastHandler, Mockito.times(1)).messageConsumer();
		Mockito.verify(broadcastHandler, Mockito.times(1)).dhtConnectionProvider();
		Mockito.verify(broadcastHandler, Mockito.times(1)).processMessage(mockMsg, job);
		Mockito.verify(msgConsumer, Mockito.times(1)).executor();
		Mockito.verify(executor, Mockito.times(1)).tryFinishProcedure(procedure);
		Mockito.verify(inputDomain, Mockito.times(1)).expectedNrOfFiles();
		Mockito.verify(bcMessage, Mockito.times(1)).inputDomain();

	}

}
