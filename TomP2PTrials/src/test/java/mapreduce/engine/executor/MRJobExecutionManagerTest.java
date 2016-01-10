package mapreduce.engine.executor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.IBCMessage;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.execution.task.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;

public class MRJobExecutionManagerTest {
	protected static Logger logger = LoggerFactory.getLogger(MRJobExecutionManagerTest.class);
	private Random random = new Random();

	@Test
	public void testDataSwitch() throws InterruptedException {
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 1);

		MRJobExecutionManager jobExecutor = MRJobExecutionManager.create(dhtConnectionProvider);
		jobExecutor.start();
		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE).addSucceedingProcedure(WordCountMapper.create(), null);
		job.incrementProcedureIndex();
		Procedure procedure = job.currentProcedure();
		String executor = "Executor_1";
		Task task = Task.create("file1");
		JobProcedureDomain inputJPD = JobProcedureDomain.create(job.id(), executor, "START", 0).tasksSize(1);
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), executor, "END", 0);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), executor, task.nextStatusIndexFor(executor), outputJPD);
		IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);
		procedure.inputDomain(inputJPD).tasks().add(task);
		for (int i = 0; i < 10; ++i) {
			context.write((i % 5 == 0 ? "where" : (i % 4 == 0 ? "is" : (i % 3 == 0 ? "hello" : (i % 2 == 0 ? "world" : "test")))), new Integer(1));
		}
		task.addOutputDomain(outputETD);
		Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					jobExecutor.transferData(procedure, task, new PriorityBlockingQueue<>());
				} else {
					logger.info("No success");
				}
			}

		}).awaitUninterruptibly();
		Thread.sleep(1000);
		assertEquals(true, task.isInProcedureDomain());

	}

	@Test
	public void testExecuteTask() throws InterruptedException {
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 5);

		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE).addSucceedingProcedure(WordCountReducer.create(), null);
		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
		Task task = Task.create("file1");
		String executor = "Executor_1"; 
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), executor, job.currentProcedure().executable().getClass().getSimpleName(),
				job.currentProcedure().procedureIndex());
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), executor, task.nextStatusIndexFor(executor), outputJPD);
		IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);

		for (int i = 0; i < 50; ++i) {
			context.write((i % 5 == 0 ? "where" : (i % 4 == 0 ? "is" : (i % 3 == 0 ? "hello" : (i % 2 == 0 ? "world" : "test")))), new Integer(1));
		}
		

		MRJobExecutionManager jobExecutor = MRJobExecutionManager.create(dhtConnectionProvider);
		jobExecutor.start();
		Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					jobExecutor.transferData(job.currentProcedure(), task, new PriorityBlockingQueue<>());
				} else {
					logger.info("No success");
				}
			}

		}).awaitUninterruptibly();
		Thread.sleep(1000);
		//
		job.incrementProcedureIndex();
		PriorityBlockingQueue<IBCMessage> bcMessages = new PriorityBlockingQueue<>();
		Procedure procedure = job.currentProcedure().inputDomain(outputJPD); 

		List<Task> tasks = new ArrayList<>();
		tasks.add(Task.create("where"));
		// tasks.add(Task.create("is"));
		// tasks.add(Task.create("hello"));
		// tasks.add(Task.create("world"));
		// tasks.add(Task.create("test"));
		for (Task t : tasks) {
			jobExecutor.executeTask(bcMessages, tasks.get(0), procedure);
		}

		Thread.sleep(2000);

	}

	@Test
	public void testExecuteJob() throws InterruptedException {
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/testFiles";

		int port = random.nextInt(10000) + 4000;
		IDHTConnectionProvider first = TestUtils.getTestConnectionProvider(port, 1);

		logger.info("before job creation");
		MRJobSubmissionManager submitter = MRJobSubmissionManager.create(first);
		Job job = Job.create(submitter.id(), PriorityLevel.MODERATE).fileInputFolderPath(fileInputFolderPath).maxFileSize(FileSize.MEGA_BYTE)
				.addSucceedingProcedure(WordCountMapper.create(), null).
				 addSucceedingProcedure(WordCountReducer.create(), null).
				nrOfSameResultHash(1);
		logger.info("before executor initialisation");
		logger.info("before executor start");
		logger.info("before submitting job");
		submitter.submit(job);
		// new Thread(new Runnable() {
		// //
		// @Override
		// public void run() {
		// IDHTConnectionProvider second = TestUtils.getTestConnectionProvider(port, 3, first.peerDHTs().get(0));
		MRJobExecutionManager jobExecutor = MRJobExecutionManager.create(first);
		jobExecutor.start();
		jobExecutor.messageConsumer().jobs().put(job, new PriorityBlockingQueue<>());

		// }
		// }).start();
		Thread.sleep(Long.MAX_VALUE);
		// first.getAll(keyString, domainString)
	}

}
