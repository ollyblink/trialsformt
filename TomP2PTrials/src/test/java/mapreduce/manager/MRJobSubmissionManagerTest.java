package mapreduce.manager;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

import mapreduce.execution.job.Job;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.DHTUtils;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;

public class MRJobSubmissionManagerTest {

	private static MRJobSubmissionManager jobSubmissionManager;

	@Test
	public void test() throws UnsupportedEncodingException {
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		DHTUtils dhtUtils = DHTUtils.newInstance(bootstrapIP, bootstrapPort);
		IDHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance(dhtUtils);
		jobSubmissionManager = MRJobSubmissionManager.newInstance(dhtConnectionProvider);

		Job job = Job.create(jobSubmissionManager.id());
		jobSubmissionManager.submit(job);
		
		dhtUtils.getKD("PROCEDURE_KEYS", DomainProvider.INSTANCE.jobProcedureDomain(job));

		// Multimap<Task, Comparable> keysForEachTask = splitter.keysForEachTask();
		// for (Task task : keysForEachTask.keySet()) {
		// Multimap<Object, Object> taskData = ArrayListMultimap.create();
		// dhtConnectionProvider.getTaskData(task, task.initialDataLocation(), taskData);
		//
		// for (Object key : taskData.keySet()) {
		// // System.err.println(job.maxFileSize() + " " + new ArrayList<Object>(taskData.get(key)).get(0).toString().getBytes("UTF-8").length);
		// assertTrue(keysForEachTask.containsValue(key));
		// assertTrue(job.maxFileSize() >= new ArrayList<Object>(taskData.get(key)).get(0).toString().getBytes("UTF-8").length);
		// }
		// }
		// jobSubmissionManager.shutdown();
	}

}
