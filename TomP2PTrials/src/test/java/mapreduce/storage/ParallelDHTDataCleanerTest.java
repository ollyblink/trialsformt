package mapreduce.storage;

import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.standardprocedures.NullMapReduceProcedure;
import mapreduce.execution.task.Task;
import mapreduce.storage.dhtmaintenance.ParallelDHTDataCleaner;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class ParallelDHTDataCleanerTest {

	private static ParallelDHTDataCleaner cleaner;
	private static Multimap<Task, Tuple<PeerAddress, Integer>> dataToRemove;
	private static DHTConnectionProvider connection;
	private static DHTConnectionProvider connection2;
	private static int bootstrapPort;
	private static String bootstrapIP;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		bootstrapIP = "192.168.43.234";
		bootstrapPort = 4000;
		IMapReduceProcedure procedure = NullMapReduceProcedure.newInstance();
		dataToRemove = ArrayListMultimap.create();
		connection = DHTConnectionProvider.newInstance(bootstrapIP, bootstrapPort).isBootstrapper(true).connect();
		Thread.sleep(1000);

		connection2 = DHTConnectionProvider.newInstance(bootstrapIP, bootstrapPort).connect();

		Task task = Task.newInstance("1").procedure(procedure);
		Task task2 = Task.newInstance("2").procedure(procedure);

		String[] data = "world hello world hello world world this is this is world hello world this".split(" ");
		dataToRemove.put(task, Tuple.create(connection.peerAddress(), -1));
		dataToRemove.put(task2, Tuple.create(connection2.peerAddress(), -1));
		for (String d : data) {
			connection.addTaskData(task, d, 1, true);
			connection2.addTaskData(task2, d, 1, true);
		}

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {

		cleaner.abortTaskExecution();
		connection.shutdown();
		connection2.shutdown();
	}

	@Test
	public void test() {
		cleaner = ParallelDHTDataCleaner.newInstance(bootstrapIP, bootstrapPort);

		System.err.println(dataToRemove);
		cleaner.removeDataFromDHT(dataToRemove);
	}

}
