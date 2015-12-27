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
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Tuple;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number640;

public class DHTStorageContextTest {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContextTest.class);
	private String executor = "EXECUTOR_1";

	@Test
	public void test() throws InterruptedException {
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(4000);
		dhtConnectionProvider.owner(executor);
		Thread.sleep(3000);

		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE);
		Task task = Task.newInstance("hello", job.id());
		task.finalDataLocationDomains(Tuple.create(executor, 0).combine());

		DHTStorageContext context = DHTStorageContext.create().dhtConnectionProvider(dhtConnectionProvider).task(task)
				.subsequentJobProcedureDomain(job.subsequentJobProcedureDomain());
		context.task().executingPeers().put(executor, BCMessageStatus.EXECUTING_TASK);

		for (int i = 0; i < 10; ++i) {
			context.write(context.task().id(), new Integer(1));
		}

		Thread.sleep(1000);

		String executorTaskDomain = job.subsequentJobProcedureDomain() + "_" + DomainProvider.INSTANCE.executorTaskDomain(context.task(),
				Tuple.create(executor, context.task().executingPeers().get(executor).size() - 1));
		System.err.println(executorTaskDomain);
		dhtConnectionProvider.getAll("hello", executorTaskDomain).addListener(new BaseFutureListener<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {

				if (future.isSuccess()) {
					System.err.println("future is success");
					if (future.dataMap() != null) {
						List<Object> values = new ArrayList<>();
						for (Number640 n : future.dataMap().keySet()) {
							Object value = ((Value) future.dataMap().get(n).object()).value();
							values.add(value);
						}
						System.err.println("<hello, " + values + ">");

						assertEquals(10, values.size());
						for (int i = 0; i < 10; ++i) {
							System.err.println("<hello, " + values.get(i) + ">");
							assertEquals(true, values.get(i) instanceof Integer);
							assertEquals(new Integer(1), ((Integer) values.get(i)));
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
		Thread.sleep(5000);
	}

}
