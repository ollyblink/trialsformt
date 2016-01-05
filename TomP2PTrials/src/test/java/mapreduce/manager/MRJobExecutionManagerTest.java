package mapreduce.manager;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import mapreduce.manager.broadcasting.broadcastmessageconsumer.AbstractMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.IBCMessage;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;

public class MRJobExecutionManagerTest {
	protected static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

	private static MRJobExecutionManager jobExecutor;
	private static IDHTConnectionProvider dhtConnectionProvider;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		// IDHTConnectionProvider con = TestUtils.getTestConnectionProvider(4000);

		// String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/manager/testFiles";

		dhtConnectionProvider = TestUtils.getTestConnectionProvider(54321, 1);
		jobExecutor = MRJobExecutionManager.create(dhtConnectionProvider);
		jobExecutor.start();

		// MRJobSubmissionManager jobSubmissionManager = MRJobSubmissionManager.newInstance(dhtConnectionProvider);

		// jobSubmissionManager.submit(job);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testDataSwitch() throws InterruptedException {
		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE);
		Task task = Task.create("file1");
		String executor = "Executor_1";
		JobProcedureDomain outputJPD = new JobProcedureDomain(job.id(), executor, "NONE", 0);
		ExecutorTaskDomain outputETD = new ExecutorTaskDomain(task.key(), executor, task.nextStatusIndexFor(executor), outputJPD);
		IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);

		for (int i = 0; i < 1000; ++i) {
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

		});
		Thread.sleep(5000);
		assertEquals(true, task.isInProcedureDomain());

	}

	@Test
	public void testExecuteTask() throws InterruptedException {
		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE).addSucceedingProcedure(WordCountReducer.create());
		Task task = Task.create("file1");
		String executor = "Executor_1";
		JobProcedureDomain outputJPD = new JobProcedureDomain(job.id(), executor, job.currentProcedure().executable().getClass().getSimpleName(),
				job.currentProcedure().procedureIndex());
		ExecutorTaskDomain outputETD = new ExecutorTaskDomain(task.key(), executor, task.nextStatusIndexFor(executor), outputJPD);
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

		});
		Thread.sleep(2000);

		job.incrementProcedureIndex();
		PriorityBlockingQueue<IBCMessage> bcMessages = new PriorityBlockingQueue<>();
		Procedure procedure = job.currentProcedure().inputDomain(outputJPD);
		JobProcedureDomain outputJPD2 = new JobProcedureDomain(job.id(), executor, job.currentProcedure().executable().getClass().getSimpleName(),
				job.currentProcedure().procedureIndex());

		List<Task> tasks = new ArrayList<>();
		tasks.add(Task.create("where"));
		tasks.add(Task.create("is"));
		tasks.add(Task.create("hello"));
		tasks.add(Task.create("world"));
		tasks.add(Task.create("test"));
		for (Task t : tasks) {
			jobExecutor.executeTask(bcMessages, t, procedure, outputJPD2);
		}

		Thread.sleep(2000);

	}

	@Ignore
	public void test() throws Exception {
		// Job job = Job.create("TEST").addSucceedingProcedure(WordCountMapper.create()).addSucceedingProcedure(WordCountReducer.create());
		// String key = "file1";
		// String value = "";
		// for (int i = 0; i < 5; ++i) {
		// value += "hello world this is me ";
		// }
		// System.err.println("To process: " + value);
		//
		// // String id = dhtConnectionProvider.owner();
		// Procedure pI = job.previousProcedure();
		//
		// Task task = Task.create("START", pI.jobProcedureDomain());
		// pI.addTask(task);
		// Tuple<String, Integer> taskExecutor = Tuple.create("SUBMITTER", 0);
		//
		// String jobProcedureDomain = pI.jobProcedureDomainString();
		// Tuple<String, Tuple<String, Integer>> executorTaskDomain = task.executorTaskDomain(taskExecutor);
		//
		// String taskExecutorDomainConcatenation = task.concatenationString(taskExecutor);
		//
		// List<FuturePut> futurePutData = SyncedCollectionProvider.syncedArrayList();
		// List<FuturePut> futurePutTEDomain = SyncedCollectionProvider.syncedArrayList();
		// List<FuturePut> futurePutProcKey = SyncedCollectionProvider.syncedArrayList();
		// futurePutData
		// .add(dhtConnectionProvider.add(key, value, taskExecutorDomainConcatenation, true).addListener(new BaseFutureAdapter<FuturePut>() {
		//
		// @Override
		// public void operationComplete(FuturePut future) throws Exception {
		// if (future.isSuccess()) {
		// futurePutTEDomain.add(dhtConnectionProvider.add(key, executorTaskDomain, jobProcedureDomain, false)
		// .addListener(new BaseFutureAdapter<FuturePut>() {
		//
		// @Override
		// public void operationComplete(FuturePut future) throws Exception {
		// if (future.isSuccess()) {
		// futurePutProcKey.add(dhtConnectionProvider.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, key,
		// jobProcedureDomain, false));
		// }
		// }
		// }));
		// }
		// }
		// }));
		//
		// Futures.whenAllSuccess(futurePutData).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {
		//
		// @Override
		// public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
		// if (future.isSuccess()) {
		// Futures.whenAllSuccess(futurePutTEDomain).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {
		//
		// @Override
		// public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
		// if (future.isSuccess()) {
		// Futures.whenAllSuccess(futurePutProcKey).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {
		//
		// @Override
		// public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
		// if (future.isSuccess()) {
		// System.err.println("Broadcast job");
		// JobDistributedBCMessage message = dhtConnectionProvider.owner("TEST").broadcastNewJob(job);
		// dhtConnectionProvider.owner(jobExecutor.id());
		// }
		// }
		// });
		//
		// }
		// }
		// });
		//
		// }
		// }
		// });
		//
		// System.err.println("Here2");
		// Thread.sleep(10000);
		// job.incrementProcedureIndex();
		// ListMultimap<String, Integer> toCheck = ArrayListMultimap.create();
		//
		// List<FutureGet> futureGetData = SyncedCollectionProvider.syncedArrayList();
		// List<FutureGet> futureGetTEDomain = SyncedCollectionProvider.syncedArrayList();
		// List<FutureGet> futureGetProcKey = SyncedCollectionProvider.syncedArrayList();
		//
		// // System.err.println(job.subsequentProcedure());
		// // job.incrementCurrentProcedureIndex();
		// // job.incrementCurrentProcedureIndex();
		// String dataLocationJobProcedureDomainString = job.currentProcedure().jobProcedureDomainString();
		// futureGetProcKey.add(dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, dataLocationJobProcedureDomainString)
		// .addListener(new BaseFutureAdapter<FutureGet>() {
		//
		// @Override
		// public void operationComplete(FutureGet future) throws Exception {
		// if (future.isSuccess()) {
		// // System.err.println("Current Procedure Domain: " + dataLocationJobProcedureDomainString);
		// if (future.dataMap() != null) {
		// for (Number640 n : future.dataMap().keySet()) {
		// String key = (String) future.dataMap().get(n).object();
		// // System.err.println("Key: " + key);
		// Task task = Task.create(key, job.currentProcedure().jobProcedureDomain());
		// futureGetTEDomain.add(dhtConnectionProvider.getAll(task.id(), dataLocationJobProcedureDomainString)
		// .addListener(new BaseFutureAdapter<FutureGet>() {
		//
		// @Override
		// public void operationComplete(FutureGet future) throws Exception {
		// if (future.isSuccess()) {
		// if (future.dataMap() != null) {
		// for (Number640 n : future.dataMap().keySet()) {
		// Tuple<String, Tuple<String, Integer>> executorTaskDomainPart = (Tuple<String, Tuple<String, Integer>>) future
		// .dataMap().get(n).object();
		// // System.err.println("Found taskExecutorDomain: " + executorTaskDomainPart);
		// String concatenationString = task.concatenationString(executorTaskDomainPart.second());
		// // System.err.println("As concatenation: " + concatenationString);
		// futureGetData.add(dhtConnectionProvider.getAll(key, concatenationString)
		// .addListener(new BaseFutureAdapter<FutureGet>() {
		//
		// @Override
		// public void operationComplete(FutureGet future) throws Exception {
		// if (future.isSuccess()) {
		// // if (future.dataMap() != null) {
		// // System.err.println("Found values");
		// // String values = "";
		// Set<Number640> keySet = future.dataMap().keySet();
		// // System.err.println("KeySet: " + keySet);
		// for (Number640 n : keySet) {
		// Integer value = (Integer) ((Value) future.dataMap().get(n).object()).value();
		// toCheck.put(key, value);
		// // values += value + ",";
		// }
		// // System.err.println(key + ": " + values);
		// // }
		// }
		// }
		// }));
		// }
		// }
		// }
		// }
		// }));
		// }
		// }
		// }
		// }
		// }));
		//
		// Futures.whenAllSuccess(futureGetProcKey).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
		//
		// @Override
		// public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
		// if (future.isSuccess()) {
		// Futures.whenAllSuccess(futureGetTEDomain).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
		//
		// @Override
		// public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
		// if (future.isSuccess()) {
		// Futures.whenAllSuccess(futureGetData).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
		//
		// @Override
		// public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
		// if (future.isSuccess()) {
		// // Assert.assertEquals(2, toCheck.keySet().size());
		// // Assert.assertEquals(true, toCheck.containsKey("hello"));
		// // Assert.assertEquals(3, toCheck.get("hello").size());
		// // for (Object o : toCheck.get("hello")) {
		// // Assert.assertEquals(true, (o instanceof Integer));
		// // Assert.assertEquals(new Integer(1), (Integer) o);
		// // }
		// // Assert.assertEquals(true, toCheck.containsKey("world"));
		// // Assert.assertEquals(6, toCheck.get("world").size());
		// // for (Object o : toCheck.get("world")) {
		// // Assert.assertEquals(true, (o instanceof Integer));
		// // Assert.assertEquals(new Integer(1), (Integer) o);
		// // }
		// for (String key : toCheck.keySet()) {
		// System.err.println(key + ": " + toCheck.get(key));
		// }
		// dhtConnectionProvider.shutdown();
		// }
		// }
		// });
		//
		// }
		// }
		// });
		//
		// }
		// }
		// });
		// Thread.sleep(10000);
	}

}
