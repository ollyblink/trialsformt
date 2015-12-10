package mapreduce.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Multimap;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.task.Task;
import mapreduce.utils.Tuple;

public class DHTConnectionProviderTest {

	private static ArrayList<IDHTConnectionProvider> connectionProviders;

	@Before
	public void setUpBeforeClass() throws Exception {
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		// String storageFilePath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/main/java/mapreduce/storage/";

		connectionProviders = new ArrayList<IDHTConnectionProvider>();
		connectionProviders.add(DHTConnectionProvider.newInstance(bootstrapIP, bootstrapPort).isBootstrapper(true));
		connectionProviders.get(0).connect();

		for (int i = 0; i < 10; ++i) {
			DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance(bootstrapIP, bootstrapPort);
			connectionProviders.add(dhtConnectionProvider);
			dhtConnectionProvider.connect();
		}
	}

	@After
	public void tearDownAfterClass() throws Exception {
		for (int i = connectionProviders.size() - 1; i >= 0; --i) {
			connectionProviders.get(i).shutdown();
		}
	}

	@Test
	public void testAddGetForOneKey() throws InterruptedException {
		int putter = 8;
		int getter = 2;
		List<Object> keys = new ArrayList<Object>();
		keys.add("this");
		keys.add("is");
		keys.add("test");

		Task task = Mockito.mock(Task.class);
		Mockito.when(task.id()).thenReturn("1");
		Mockito.when(task.jobId()).thenReturn("1");
		IMapReduceProcedure procedure = Mockito.mock(IMapReduceProcedure.class);
		Mockito.when(task.procedure()).thenReturn(procedure);
		for (Object o : keys) {
			connectionProviders.get(putter).addTaskData(task, o, new Integer(1), true);
		}
		// Thread.sleep(1000);

		LocationBean location = LocationBean.create(Tuple.create(connectionProviders.get(putter).peerAddress(), -1), procedure);
		Multimap<Object, Object> dataForTask = connectionProviders.get(getter).getTaskData(task, location);
		System.err.println(dataForTask.keySet().size());

		assertTrue(dataForTask.keySet().size() == 3);
		assertTrue(dataForTask.values().size() == 3);
		assertTrue(dataForTask.keySet().contains("this"));
		assertTrue(new ArrayList<Object>(dataForTask.get("this")).get(0).equals(new Integer(1)));
		assertTrue(dataForTask.keySet().contains("is"));
		assertTrue(new ArrayList<Object>(dataForTask.get("is")).get(0).equals(new Integer(1)));
		assertTrue(dataForTask.keySet().contains("test"));
		assertTrue(new ArrayList<Object>(dataForTask.get("test")).get(0).equals(new Integer(1)));
	}

	@Test
	public void testAddGetForMultipleSameKeysAndSameValues() throws InterruptedException {
		int putter = 8;
		int getter = 2;
		List<Object> keys = new ArrayList<Object>();
		keys.add("this");
		keys.add("is");
		keys.add("test");
		keys.add("this");
		keys.add("is");
		keys.add("test");
		keys.add("this");
		keys.add("is");
		keys.add("test");

		Task task = Mockito.mock(Task.class);
		Mockito.when(task.id()).thenReturn("1");
		Mockito.when(task.jobId()).thenReturn("1");
		IMapReduceProcedure procedure = Mockito.mock(IMapReduceProcedure.class);
		Mockito.when(task.procedure()).thenReturn(procedure);
		for (Object o : keys) {
			connectionProviders.get(putter).addTaskData(task, o, new Integer(1), true);
		}
		// Thread.sleep(1000);
		LocationBean location = LocationBean.create(Tuple.create(connectionProviders.get(putter).peerAddress(), -1), procedure);
		Multimap<Object, Object> dataForTask = connectionProviders.get(getter).getTaskData(task, location);

		System.err.println(dataForTask);
		assertEquals(3, dataForTask.keySet().size());
		assertEquals(9, dataForTask.values().size());
		assertTrue(dataForTask.keySet().contains("this"));
		assertTrue(new ArrayList<Object>(dataForTask.get("this")).get(0).equals(new Integer(1)));
		assertTrue(new ArrayList<Object>(dataForTask.get("this")).get(1).equals(new Integer(1)));
		assertTrue(new ArrayList<Object>(dataForTask.get("this")).get(2).equals(new Integer(1)));
		assertTrue(dataForTask.keySet().contains("is"));
		assertTrue(new ArrayList<Object>(dataForTask.get("is")).get(0).equals(new Integer(1)));
		assertTrue(new ArrayList<Object>(dataForTask.get("is")).get(1).equals(new Integer(1)));
		assertTrue(new ArrayList<Object>(dataForTask.get("is")).get(2).equals(new Integer(1)));
		assertTrue(dataForTask.keySet().contains("test"));
		assertTrue(new ArrayList<Object>(dataForTask.get("test")).get(0).equals(new Integer(1)));
		assertTrue(new ArrayList<Object>(dataForTask.get("test")).get(1).equals(new Integer(1)));
		assertTrue(new ArrayList<Object>(dataForTask.get("test")).get(2).equals(new Integer(1)));
	}

	@Test
	public void testAddGetForMultipleSameKeysAndDifferentValues() throws InterruptedException {
		int putter = 8;
		int getter = 2;
		List<Object> keys = new ArrayList<Object>();
		keys.add("this");
		keys.add("is");
		keys.add("test");
		keys.add("this");
		keys.add("is");
		keys.add("test");
		keys.add("this");
		keys.add("is");
		keys.add("test");

		Task task = Mockito.mock(Task.class);
		Mockito.when(task.id()).thenReturn("1");
		Mockito.when(task.jobId()).thenReturn("1");
		IMapReduceProcedure procedure = Mockito.mock(IMapReduceProcedure.class);
		Mockito.when(task.procedure()).thenReturn(procedure);
		int cntr = 0;
		for (Object o : keys) {
			connectionProviders.get(putter).addTaskData(task, o, new Integer(++cntr), true);
		}
		// Thread.sleep(1000);
		LocationBean location = LocationBean.create(Tuple.create(connectionProviders.get(putter).peerAddress(), -1), procedure);
		Multimap<Object, Object> dataForTask = connectionProviders.get(getter).getTaskData(task, location);

		System.err.println(dataForTask);
		assertEquals(3, dataForTask.keySet().size());
		assertEquals(9, dataForTask.values().size());
		assertTrue(dataForTask.keySet().contains("this"));
		assertTrue((dataForTask.get("this")).contains(new Integer(1)));
		assertTrue((dataForTask.get("this")).contains(new Integer(4)));
		assertTrue((dataForTask.get("this")).contains(new Integer(7)));
		assertTrue(dataForTask.keySet().contains("is"));
		assertTrue((dataForTask.get("is")).contains(new Integer(2)));
		assertTrue((dataForTask.get("is")).contains(new Integer(5)));
		assertTrue((dataForTask.get("is")).contains(new Integer(8)));
		assertTrue(dataForTask.keySet().contains("test"));
		assertTrue((dataForTask.get("test")).contains(new Integer(3)));
		assertTrue((dataForTask.get("test")).contains(new Integer(6)));
		assertTrue((dataForTask.get("test")).contains(new Integer(9)));
	}

	@Test
	public void testRemoveData() {
		int putter = 8;
		int getter = 2;
		List<Object> keys = new ArrayList<Object>();
		keys.add("this");
		keys.add("is");
		keys.add("test");
		keys.add("this");
		keys.add("is");
		keys.add("test");
		keys.add("this");
		keys.add("is");
		keys.add("test");

		Task task = Mockito.mock(Task.class);
		Mockito.when(task.id()).thenReturn("1");
		Mockito.when(task.jobId()).thenReturn("1");
		IMapReduceProcedure procedure = Mockito.mock(IMapReduceProcedure.class);
		Mockito.when(task.procedure()).thenReturn(procedure);
		int cntr = 0;
		for (Object o : keys) {
			connectionProviders.get(putter).addTaskData(task, o, new Integer(++cntr), true);
		}
		LocationBean location = LocationBean.create(Tuple.create(connectionProviders.get(putter).peerAddress(), -1), procedure);
		Multimap<Object, Object> dataForTask = connectionProviders.get(getter).getTaskData(task, location);

		System.err.println(dataForTask);
		assertEquals(3, dataForTask.keySet().size());
		assertEquals(9, dataForTask.values().size());
		assertTrue(dataForTask.keySet().contains("this"));
		assertTrue((dataForTask.get("this")).contains(new Integer(1)));
		assertTrue((dataForTask.get("this")).contains(new Integer(4)));
		assertTrue((dataForTask.get("this")).contains(new Integer(7)));
		assertTrue(dataForTask.keySet().contains("is"));
		assertTrue((dataForTask.get("is")).contains(new Integer(2)));
		assertTrue((dataForTask.get("is")).contains(new Integer(5)));
		assertTrue((dataForTask.get("is")).contains(new Integer(8)));
		assertTrue(dataForTask.keySet().contains("test"));
		assertTrue((dataForTask.get("test")).contains(new Integer(3)));
		assertTrue((dataForTask.get("test")).contains(new Integer(6)));
		assertTrue((dataForTask.get("test")).contains(new Integer(9)));

		connectionProviders.get(0).removeTaskResultsFor(task, location);
		dataForTask = connectionProviders.get(getter).getTaskData(task, location);

		System.err.println(dataForTask);
		assertEquals(0, dataForTask.keySet().size());
		assertEquals(0, dataForTask.values().size());
	}
}
