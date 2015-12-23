package mapreduce.execution.computation.context;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.DHTUtils;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Tuple;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number640;

public class DHTStorageContextTest {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContextTest.class);

	@Test
	public void test() throws InterruptedException {
		String bootstrapIP = "192.168.43.65";
		int bootstrapPort = 4000;
		DHTUtils dhtUtils = DHTUtils.newInstance(bootstrapIP, bootstrapPort);
		IDHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance(dhtUtils);
		dhtConnectionProvider.connect();
		Thread.sleep(3000);

		DHTStorageContext context = DHTStorageContext.create(dhtConnectionProvider).task(Task.newInstance("hello", "1"));
		context.task().executingPeers().put(dhtUtils.peerAddress(), BCMessageStatus.EXECUTING_TASK);
		for (int i = 0; i < 10; ++i) {
			context.write(context.task().id(), new Integer(1));
		}

		Thread.sleep(3000);

		dhtConnectionProvider
				.getAll("hello",
						DomainProvider.INSTANCE.executorTaskDomain(context.task(),
								Tuple.create(dhtUtils.peerAddress(), context.task().executingPeers().get(dhtUtils.peerAddress()).size() - 1)))
				.addListener(new BaseFutureListener<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
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
	}

}
