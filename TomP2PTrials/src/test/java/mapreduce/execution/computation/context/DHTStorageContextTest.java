package mapreduce.execution.computation.context;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Tuple;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class DHTStorageContextTest {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContextTest.class);
	private String executor = "EXECUTOR_1";

	@Test
	public void test() throws InterruptedException {
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(4000, 3);
		dhtConnectionProvider.owner(executor);
		Thread.sleep(3000);

		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE);
		Task task = Task.create("hello", job.previousProcedure().jobProcedureDomain());
		Tuple<String, Integer> taskExecutor = Tuple.create(executor, 0);
		task.addFinalExecutorTaskDomainPart(taskExecutor);

		IContext context = DHTStorageContext.create().taskExecutor(taskExecutor).task(task).dhtConnectionProvider(dhtConnectionProvider)
				.subsequentProcedure(job.currentProcedure());
		context.task().executingPeers().put(executor, BCMessageStatus.EXECUTING_TASK);

		for (int i = 0; i < 10; ++i) {
			context.write(context.task().id(), new Integer(1));
		}
		List<Object> values = SyncedCollectionProvider.syncedArrayList();
		Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				String executorTaskDomain = task.concatenationString(taskExecutor);
				System.err.println(executorTaskDomain);
				dhtConnectionProvider.getAll("hello", executorTaskDomain).addListener(new BaseFutureListener<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {

						if (future.isSuccess()) {
							System.err.println("future is success");
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

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						logger.warn("Exception thrown", t);
					}
				});
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
