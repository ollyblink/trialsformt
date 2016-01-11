package mapreduce.execution.context;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.engine.messageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.task.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class DHTStorageContextTest {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContextTest.class);
	private String executor = "EXECUTOR_1";

	@Test
	public void test() throws InterruptedException {
		MRJobExecutionManager jobExecutionManager = MRJobExecutionManager.create();
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(4000, 3,
				MRJobExecutionManagerMessageConsumer.create(jobExecutionManager));
		dhtConnectionProvider.executor(executor);
		Thread.sleep(3000);

		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE);
		Task task = Task.create("hello");
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), executor, "NONE", 0);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), executor, task.newStatusIndex(), outputJPD);
		IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);

		for (int i = 0; i < 10; ++i) {
			context.write(task.key(), new Integer(1));
		}
		List<Object> values = SyncedCollectionProvider.syncedArrayList();
		Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					dhtConnectionProvider.getAll("hello", outputETD.toString()).addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								logger.info("future is success");
								if (future.dataMap() != null) {
									for (Number640 n : future.dataMap().keySet()) {
										Object value = ((Value) future.dataMap().get(n).object()).value();
										values.add(value);
									}
								}
							} else {
								logger.warn("No success on retrieving values for 'hello'");
							}
						}

					});
				} else {
					logger.warn("No success on putting data");
				}
			}
		});
		while (values.size() != 10) {
			Thread.sleep(1000);
		}
		assertEquals(10, values.size());
		for (int i = 0; i < 10; ++i) {
			assertEquals(true, values.get(i) instanceof Integer);
			assertEquals(new Integer(1), ((Integer) values.get(i)));
		}

	}

}
