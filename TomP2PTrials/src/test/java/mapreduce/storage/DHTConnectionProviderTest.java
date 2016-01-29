package mapreduce.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import generictests.MyBroadcastHandler;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class DHTConnectionProviderTest {
	private IDHTConnectionProvider dht = DHTConnectionProvider.INSTANCE;

	@Before
	public void init() {
		TestUtils.getTestConnectionProvider();
	}

	@Test
	public void testAddAllGetAll() {
		dht.put("Hello", 1, "hello domain").awaitUninterruptibly();
		dht.get("Hello", "hello domain").awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					Integer integer = (Integer) future.data().object();
					assertEquals(new Integer(1), integer);
					System.err.println(integer);
				} else {
					fail();
				}
			}
		});
	}

	@Test
	public void testPutGet() {
		dht.put("Hello", 1, "hello domain").awaitUninterruptibly();
		dht.get("Hello", "hello domain").awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					Integer integer = (Integer) future.data().object();
					assertEquals(new Integer(1), integer);
					System.err.println(integer);
				} else {
					fail();
				}
			}
		});
		dht.shutdown();
	}

	@Test
	public void testAddAsListGetAll() {
		dht.add("Hello", 1, "hello domain", true).awaitUninterruptibly();
		dht.add("Hello", 1, "hello domain", true).awaitUninterruptibly();
		dht.add("Hello", 1, "hello domain", true).awaitUninterruptibly();
		dht.getAll("Hello", "hello domain").awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {

				if (future.isSuccess()) {
					Map<Number640, Data> dataMap = future.dataMap();
					Set<Number640> keySet = dataMap.keySet();
					assertEquals(3, keySet.size());
					for (Number640 n : keySet) {
						Integer integer = (Integer) ((Value) dataMap.get(n).object()).value();
						assertEquals(new Integer(1), integer);
					}
				} else {
					fail();
				}
			}
		});
		dht.shutdown();
	}

	@Test
	public void testAddAsSetGetAll() {
		dht.add("Hello", 1, "hello domain", false).awaitUninterruptibly();
		dht.add("Hello", 1, "hello domain", false).awaitUninterruptibly();
		dht.add("Hello", 1, "hello domain", false).awaitUninterruptibly();
		dht.getAll("Hello", "hello domain").awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {

				if (future.isSuccess()) {
					Map<Number640, Data> dataMap = future.dataMap();
					Set<Number640> keySet = dataMap.keySet();
					assertEquals(1, keySet.size());
					for (Number640 n : keySet) {
						Integer integer = (Integer) (dataMap.get(n).object()); 
						assertEquals(new Integer(1), integer);
					}
				} else {
					fail();
				}
			}
		});
		dht.shutdown();
	}

	@Test
	public void testAddAllAsSetGetAll() throws IOException {
		Collection<Data> data = new ArrayList<>();
		data.add(new Data(new Integer(1)));
		data.add(new Data(new Integer(1)));
		data.add(new Data(new Integer(1)));
		dht.addAll("Hello", data, "hello domain").awaitUninterruptibly();
		dht.getAll("Hello", "hello domain").awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {

				if (future.isSuccess()) {
					Map<Number640, Data> dataMap = future.dataMap();
					Set<Number640> keySet = dataMap.keySet();
					assertEquals(1, keySet.size());
					for (Number640 n : keySet) {
						Integer integer = (Integer) (dataMap.get(n).object());
						assertEquals(new Integer(1), integer);
					}
				} else {
					fail();
				}
			}
		});
		dht.shutdown();
	}

	@Test
	public void testAddAllAsListGetAll() throws IOException {
		Collection<Data> data = new ArrayList<>();
		data.add(new Data(new Value(new Integer(1))));
		data.add(new Data(new Value(new Integer(1))));
		data.add(new Data(new Value(new Integer(1))));
		dht.addAll("Hello", data, "hello domain").awaitUninterruptibly();
		dht.getAll("Hello", "hello domain").awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {

				if (future.isSuccess()) {
					Map<Number640, Data> dataMap = future.dataMap();
					Set<Number640> keySet = dataMap.keySet();
					assertEquals(3, keySet.size());
					for (Number640 n : keySet) {
						Integer integer = (Integer) ((Value) dataMap.get(n).object()).value();
						assertEquals(new Integer(1), integer);
					}
				} else {
					fail();
				}
			}
		});
		dht.shutdown();
	}
 
}
