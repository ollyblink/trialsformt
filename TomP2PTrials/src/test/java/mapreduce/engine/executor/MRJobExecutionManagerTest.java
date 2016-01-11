package mapreduce.engine.executor;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.messageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.execution.task.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class MRJobExecutionManagerTest {
	protected static Logger logger = LoggerFactory.getLogger(MRJobExecutionManagerTest.class);
	private Random random = new Random();

	@Test
	public void testDataSwitch() throws InterruptedException {
		MRJobExecutionManagerMessageConsumer msgConsumer = MRJobExecutionManagerMessageConsumer.create();
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 1, msgConsumer);
		msgConsumer.dhtConnectionProvider(dhtConnectionProvider);
		MRJobExecutionManager jobExecutor = msgConsumer.getExecutor();

		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE).addSucceedingProcedure(WordCountMapper.create(), null, 1, 1);
		// jobExecutor.messageConsumer().jobs().put(job, new PriorityBlockingQueue<>());
		dhtConnectionProvider.broadcastHandler().jobFutures().put(job, null);
		// dhtConnectionProvider.broadcastHandler().jobFutures().get(job).clear();
		job.incrementProcedureIndex();
		Procedure procedure = job.currentProcedure();
		String executor = "Executor_1";
		Task task = Task.create("file1");
		JobProcedureDomain inputJPD = JobProcedureDomain.create(job.id(), executor, "START", 0).tasksSize(1);
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), executor, WordCountMapper.class.getSimpleName(), 1);

		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), executor, task.newStatusIndex(), outputJPD);
		IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);
		procedure.inputDomain(inputJPD).tasks().add(task);
		for (int i = 0; i < 1000; ++i) {
			context.write((i % 5 == 0 ? "where" : (i % 4 == 0 ? "is" : (i % 3 == 0 ? "hello" : (i % 2 == 0 ? "world" : "test")))), new Integer(1));
		}
		task.addOutputDomain(outputETD);
		procedure.inputDomain(inputJPD);
		Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					jobExecutor.switchDataFromTaskToProcedureDomain(procedure, task);
				} else {
					logger.info("No success");
				}
			}

		}).awaitUninterruptibly();
		Thread.sleep(2000);
		assertEquals(true, task.isFinished());
		assertEquals(true, task.isInProcedureDomain());
		assertEquals(true, procedure.isFinished());
		assertEquals(1, procedure.nrOfOutputDomains());
		// Thread.sleep(1000);
	}

	@Test
	public void testExecuteTaskWithoutCombiner() throws InterruptedException {
		testExecuteTask("test is test is test is test is is is test test test", null, 7, 6, 7, 6);
	}

	@Test
	public void testExecuteTaskWithCombiner() throws InterruptedException {
		testExecuteTask("test test test is test test test test is is is is is is is test test is is", WordCountReducer.create(), 1, 1, 9, 10);
	}

	private void testExecuteTask(String testIsText, IExecutable combiner, int testCount, int isCount, int testSum, int isSum)
			throws InterruptedException {

		MRJobExecutionManagerMessageConsumer msgConsumer = MRJobExecutionManagerMessageConsumer.create();
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 1, msgConsumer);
		msgConsumer.dhtConnectionProvider(dhtConnectionProvider);
		MRJobExecutionManager jobExecutor = msgConsumer.getExecutor();

		Job job = Job.create("SUBMITTER");
		dhtConnectionProvider.broadcastHandler().jobFutures().put(job, null);

		JobProcedureDomain dataDomain = JobProcedureDomain.create(job.id(), jobExecutor.id(), StartProcedure.class.getSimpleName(), 0).tasksSize(1);
		addTaskDataToProcedureDomain(dhtConnectionProvider, "file1", testIsText, dataDomain.toString());
		Procedure procedure = Procedure.create(WordCountMapper.create(), 1).inputDomain(dataDomain).combiner(combiner);

		jobExecutor.executeTask(Task.create("file1"), procedure);

		Thread.sleep(2000);
		JobProcedureDomain outputJPD = JobProcedureDomain
				.create(procedure.inputDomain().jobId(), jobExecutor.id(), procedure.executable().getClass().getSimpleName(), 1)
				.nrOfFinishedTasks(procedure.nrOfFinishedTasks());
		Number160 resultHash = Number160.ZERO;
		if (combiner == null) {
			for (int i = 0; i < testSum; ++i) {
				resultHash = resultHash.xor(Number160.createHash("test")).xor(Number160.createHash(new Integer(1).toString()));
			}
			for (int i = 0; i < isSum; ++i) {
				resultHash = resultHash.xor(Number160.createHash("is")).xor(Number160.createHash(new Integer(1).toString()));
			}
		} else {
			resultHash = resultHash.xor(Number160.createHash("test")).xor(Number160.createHash(new Integer(testSum).toString()));
			resultHash = resultHash.xor(Number160.createHash("is")).xor(Number160.createHash(new Integer(isSum).toString()));
		}
		logger.info("Expected result hash: " + resultHash);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create("file1", jobExecutor.id(), 0, outputJPD).resultHash(resultHash);

		// assertEquals(1, bcMessages.size());
		// CompletedBCMessage msg = (CompletedBCMessage) bcMessages.take();
		// assertEquals(BCMessageStatus.COMPLETED_TASK, msg.status());
		// assertEquals(dataDomain, msg.inputDomain());
		// assertEquals(outputETD, msg.outputDomain());

		logger.info("Output ExecutorTaskDomain: " + outputETD.toString());
		dhtConnectionProvider.getAll("test", outputETD.toString()).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					Set<Number640> keySet = future.dataMap().keySet();
					assertEquals(testCount, keySet.size());
					int sum = 0;
					for (Number640 keyHash : keySet) {
						sum += (Integer) ((Value) future.dataMap().get(keyHash).object()).value();

					}
					assertEquals(testSum, sum);
					logger.info("test: " + sum);
				}
			}

		}).awaitUninterruptibly();
		dhtConnectionProvider.getAll("is", outputETD.toString()).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					Set<Number640> keySet = future.dataMap().keySet();
					assertEquals(isCount, keySet.size());
					int sum = 0;
					for (Number640 keyHash : keySet) {
						sum += (Integer) ((Value) future.dataMap().get(keyHash).object()).value();

					}
					assertEquals(isSum, sum);
					logger.info("is: " + sum);
				}
			}

		}).awaitUninterruptibly();
		Thread.sleep(2000);
	}

	private void addTaskDataToProcedureDomain(IDHTConnectionProvider dhtConnectionProvider, Object keyOut, Object valueOut, String oETDString) {
		List<FuturePut> futurePutData = SyncedCollectionProvider.syncedArrayList();
		futurePutData.add(dhtConnectionProvider.add(keyOut.toString(), valueOut, oETDString, true).addListener(new BaseFutureAdapter<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					logger.info(" Successfully performed add(" + keyOut.toString() + ", " + valueOut.toString() + ").domain(" + oETDString + ")");
				} else {
					logger.info("Failed to perform add(" + keyOut.toString() + ", " + valueOut.toString() + ").domain(" + oETDString + ")");
				}
			}
		}));
		futurePutData.add(dhtConnectionProvider.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, keyOut.toString(), oETDString, false)
				.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							logger.info("Successfully performed add(" + DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS + ", " + keyOut.toString()
									+ ").domain(" + oETDString + ")");
						} else {

							logger.warn("Failed to perform add(" + DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS + ", " + keyOut.toString()
									+ ").domain(" + oETDString + ")");
						}
					}
				}));
		Futures.whenAllSuccess(futurePutData).awaitUninterruptibly();
	}

	@Ignore
	public void testExecuteJob() throws InterruptedException {
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/testFiles";

		// int port = random.nextInt(10000) + 4000;
		// MRJobExecutionManager jobExecutor = MRJobExecutionManager.create();
		MRJobExecutionManagerMessageConsumer msgConsumer = MRJobExecutionManagerMessageConsumer.create();
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 1, msgConsumer);
		// msgConsumer.dhtConnectionProvider(dhtConnectionProvider);

		logger.info("before job creation");
		MRJobSubmissionManager submitter = MRJobSubmissionManager.create(dhtConnectionProvider);
		Job job = Job.create(submitter.id(), PriorityLevel.MODERATE).fileInputFolderPath(fileInputFolderPath).maxFileSize(FileSize.TWO_MEGA_BYTES)
				.addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1);

		submitter.submit(job);
		// new Thread(new Runnable() {
		// //
		// @Override
		// public void run() {
		// IDHTConnectionProvider second = TestUtils.getTestConnectionProvider(port, 3, first.peerDHTs().get(0));
		logger.info("After submission");
		// MRJobExecutionManager jobExecutor = MRJobExecutionManager.create(dhtConnectionProvider);
		// jobExecutor.start();

		// dhtConnectionProvider.broadcastHandler().jobs().add(job);
		// jobExecutor.messageConsumer().jobs().put(job, new PriorityBlockingQueue<>());

		// }
		// }).start();
		Thread.sleep(Long.MAX_VALUE);
		// first.getAll(keyString, domainString)
	}

}
