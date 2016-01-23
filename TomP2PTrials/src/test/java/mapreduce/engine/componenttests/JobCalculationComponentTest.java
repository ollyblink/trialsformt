package mapreduce.engine.componenttests;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import net.tomp2p.futures.Futures;

public class JobCalculationComponentTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {

		JobCalculationExecutor calculationExecutor = JobCalculationExecutor.create();

		JobCalculationMessageConsumer calculationMessageConsumer = JobCalculationMessageConsumer.create()
				.executor(calculationExecutor);

		JobCalculationBroadcastHandler executorBCHandler = JobCalculationBroadcastHandler.create()
				.messageConsumer(calculationMessageConsumer);
		// int bootstrapPort = 4001;
		IDHTConnectionProvider dhtCon = TestUtils.getTestConnectionProvider(executorBCHandler);
				// DHTConnectionProvider
				// .create("192.168.43.65", bootstrapPort, bootstrapPort).broadcastHandler(executorBCHandler)
				// .storageFilePath("C:\\Users\\Oliver\\Desktop\\storage")
		;
		dhtCon.broadcastHandler(executorBCHandler);
		calculationExecutor.dhtConnectionProvider(dhtCon);
		calculationMessageConsumer.dhtConnectionProvider(dhtCon);
//		executorBCHandler.dhtConnectionProvider(dhtCon);

		String fileInputFolderPath = System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/testFiles";
		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES)
				.fileInputFolderPath(fileInputFolderPath).addSucceedingProcedure(WordCountMapper.create(),
						WordCountReducer.create(), 1, 1, false, false).timeToLive(Long.MAX_VALUE)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);
		dhtCon.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
		Procedure procedure = job.currentProcedure();
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), job.submissionCount(), "S1",
				procedure.executable().getClass().getSimpleName(), procedure.procedureIndex());
		procedure.dataInputDomain(JobProcedureDomain
				.create(job.id(), job.submissionCount(), "S1", DomainProvider.INITIAL_PROCEDURE, -1)
				.expectedNrOfFiles(4)).addOutputDomain(outputJPD);

		List<Task> tasks = new ArrayList<>();
		tasks.add(Task.create("testfile1", "S1"));
		// tasks.add(Task.create("testfile2", "S1"));
		List<String> values = new ArrayList<>();
		values.add("hello world hello world hello world");
		// values.add("the quick fox jumps over the lazy dog");
		List<IBCMessage> msgs = new ArrayList<>();
		int counter = 0;
		for (Task task : tasks) {
			ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), "S1", task.newStatusIndex(),
					outputJPD);
			IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD)
					.dhtConnectionProvider(dhtCon);

			context.write(task.key(), values.get(counter++));
			Futures.whenAllSuccess(context.futurePutData()).awaitUninterruptibly();
			outputETD.resultHash(context.resultHash());
			IBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(outputETD,
					procedure.dataInputDomain());
			msgs.add(msg);
		}

		for (IBCMessage msg : msgs) {
			dhtCon.broadcastCompletion(msg);
		}
		while (!dhtCon.connect().peer().isShutdown()) {
			Thread.sleep(Long.MAX_VALUE);
		}
	}

}
