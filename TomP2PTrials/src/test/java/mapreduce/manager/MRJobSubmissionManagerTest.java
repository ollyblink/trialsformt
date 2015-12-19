package mapreduce.manager;

import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.DHTUtils;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.MaxFileSizeFileSplitter;

public class MRJobSubmissionManagerTest {

	private static MRJobSubmissionManager jobSubmissionManager;

	@Test
	public void test() throws UnsupportedEncodingException {
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		IDHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance(DHTUtils.newInstance(bootstrapIP, bootstrapPort));
		jobSubmissionManager = MRJobSubmissionManager.newInstance(dhtConnectionProvider);
		Job job = TestUtils.testJobWO(WordCountMapper.newInstance());

		jobSubmissionManager.submit(job, true);

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
		jobSubmissionManager.shutdown();
	}

}
