package mapreduce.storage;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.KeyValuePair;
import mapreduce.execution.jobtask.Task;
import mapreduce.utils.IDCreator;

public class DHTConnectionProviderTest {

	private static DHTConnectionProvider dhtConnectionProvider;
	private static DHTConnectionProvider dhtConnectionProvider2;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		// String storageFilePath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/main/java/mapreduce/storage/";
		dhtConnectionProvider = DHTConnectionProvider.newDHTConnectionProvider().port(bootstrapPort);
		dhtConnectionProvider.connect();
		dhtConnectionProvider2 = DHTConnectionProvider.newDHTConnectionProvider().bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort);
		dhtConnectionProvider2.connect();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		dhtConnectionProvider2.shutdown();
		dhtConnectionProvider.shutdown();
	}

	@Test
	public void testAddGet() {
		List<Object> keys = new ArrayList<Object>();
		keys.add(JobStatus.EXECUTING_TASK);
		keys.add(JobStatus.FINISHED_ALL_TASKS);
		keys.add(JobStatus.DISTRIBUTED_JOB);
		Task task = Task.newInstance(IDCreator.INSTANCE.createTimeRandomID(Job.class.getSimpleName())).keys(keys);
		for (Object o : keys) {
			dhtConnectionProvider.addDataForTask(task.id(), o, 1);
		}

		final List<KeyValuePair<Object, Object>> dataForTask = dhtConnectionProvider.getDataForTask(task);
		assertTrue(dataForTask.size() == 3);
		assertTrue(dataForTask.get(0).key().equals(JobStatus.EXECUTING_TASK));
		assertTrue(dataForTask.get(0).value().equals(new Integer(1)));
		assertTrue(dataForTask.get(1).key().equals(JobStatus.FINISHED_ALL_TASKS));
		assertTrue(dataForTask.get(1).value().equals(new Integer(1)));
		assertTrue(dataForTask.get(2).key().equals(JobStatus.DISTRIBUTED_JOB));
		assertTrue(dataForTask.get(2).value().equals(new Integer(1)));
	}

}
