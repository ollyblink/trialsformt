package mapreduce.manager;

import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.junit.Test;

import com.google.common.collect.Multimap;

import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.tasksplitting.MaxFileSizeTaskSplitter;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;

public class MRJobSubmissionManagerTest {

	private static MRJobSubmissionManager jobSubmissionManager;

	@Test
	public void test() throws UnsupportedEncodingException {
		String bootstrapIP = "192.168.43.234";
		IDHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance(bootstrapIP, 4000);
		jobSubmissionManager = MRJobSubmissionManager.newInstance(dhtConnectionProvider);
		Job job = TestUtils.testJobWO(new WordCountMapper());
		MaxFileSizeTaskSplitter splitter = MaxFileSizeTaskSplitter.newInstance();
		jobSubmissionManager.taskSplitter(splitter);
		jobSubmissionManager.submit(job, true);
		Multimap<Task, Comparable> keysForEachTask = splitter.keysForEachTask();
		for (Task task : keysForEachTask.keySet()) {
			Multimap<Object, Object> taskData = dhtConnectionProvider.getTaskData(task, task.initialDataLocation(), true);

			for (Object key : taskData.keySet()) {
				// System.err.println(job.maxFileSize() + " " + new ArrayList<Object>(taskData.get(key)).get(0).toString().getBytes("UTF-8").length);
				assertTrue(keysForEachTask.containsValue(key));
				assertTrue(job.maxFileSize() >= new ArrayList<Object>(taskData.get(key)).get(0).toString().getBytes("UTF-8").length);
			}
		}
		jobSubmissionManager.shutdown();
	}

}
