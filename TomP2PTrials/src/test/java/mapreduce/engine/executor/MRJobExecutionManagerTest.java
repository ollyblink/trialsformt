package mapreduce.engine.executor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.IBCMessage;
import mapreduce.engine.broadcasting.MRBroadcastHandler;
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
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;

public class MRJobExecutionManagerTest {
	protected static Logger logger = LoggerFactory.getLogger(MRJobExecutionManagerTest.class);
	private Random random = new Random();

	@Ignore
	public void testDataSwitch() throws InterruptedException {
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 5);

		MRJobExecutionManager jobExecutor = MRJobExecutionManager.create(dhtConnectionProvider);
		jobExecutor.start();
		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE);
		Task task = Task.create("file1");
		String executor = "Executor_1";
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), executor, "NONE", 0);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), executor, task.nextStatusIndexFor(executor), outputJPD);
		IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);

		for (int i = 0; i < 1000; ++i) {
			context.write((i % 5 == 0 ? "where" : (i % 4 == 0 ? "is" : (i % 3 == 0 ? "hello" : (i % 2 == 0 ? "world" : "test")))), new Integer(1));
		}
		Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					
					jobExecutor.transferData(jobExecutor.dhtConnectionProvider().broadcastHandler().jobQueues().get(job.id()), task, outputJPD);
				} else {
					logger.info("No success");
				}
			}

		});
		Thread.sleep(5000);
		assertEquals(true, task.isInProcedureDomain());

	}

	@Ignore
	public void testExecuteTask() throws InterruptedException {
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 5);

		MRJobExecutionManager jobExecutor = MRJobExecutionManager.create(dhtConnectionProvider);
		jobExecutor.start();
		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE).addSucceedingProcedure(WordCountReducer.create());
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
		Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					jobExecutor.transferDataFromETDtoJPD(task, (ExecutorTaskDomain) outputETD, outputJPD);
				} else {
					logger.info("No success");
				}
			}

		}).awaitUninterruptibly();
		//
		job.incrementProcedureIndex();
		PriorityBlockingQueue<IBCMessage> bcMessages = new PriorityBlockingQueue<>();
		Procedure procedure = job.currentProcedure().inputDomain(outputJPD);
		JobProcedureDomain outputJPD2 = JobProcedureDomain.create(job.id(), executor, job.currentProcedure().executable().getClass().getSimpleName(),
				job.currentProcedure().procedureIndex());

		List<Task> tasks = new ArrayList<>();
		tasks.add(Task.create("where"));
		// tasks.add(Task.create("is"));
		// tasks.add(Task.create("hello"));
		// tasks.add(Task.create("world"));
		// tasks.add(Task.create("test"));
		for (Task t : tasks) {
			jobExecutor.executeTask(bcMessages, t, procedure, outputJPD2);
		}

		Thread.sleep(2000);

	}

	@Test
	public void testExecuteJob() throws InterruptedException {
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/testFiles";
		int port = random.nextInt(10000) + 4000;
		logger.info("before submitter initialisation");
		IDHTConnectionProvider first = TestUtils.getTestConnectionProvider(port, 1);

		logger.info("before job creation");
		MRJobSubmissionManager submitter = MRJobSubmissionManager.create(first);
		Job job = Job.create(submitter.id(), PriorityLevel.MODERATE).fileInputFolderPath(fileInputFolderPath)
				.addSucceedingProcedure(WordCountMapper.create()).addSucceedingProcedure(WordCountReducer.create()).nrOfSameResultHash(1);
		logger.info("before executor initialisation");
		logger.info("before executor start");
		logger.info("before submitting job");
		submitter.submit(job);
		// new Thread(new Runnable() {
		//
		// @Override
		// public void run() {
		// IDHTConnectionProvider second = TestUtils.getTestConnectionProvider(port, 3, first.peerDHTs().get(0));
		MRJobExecutionManager jobExecutor = MRJobExecutionManager.create(first).maxNrOfExecutions(1);
		jobExecutor.start();
		jobExecutor.dhtConnectionProvider().broadcastHandler().jobQueues().put(job, new PriorityBlockingQueue<>());
		// }
		// });
		Thread.sleep(Long.MAX_VALUE);
	}

}
