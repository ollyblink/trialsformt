package mapreduce.server;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.execution.jobtask.Task;
import mapreduce.storage.IDHTConnectionProvider;

public class MRJobExecutorTest {

	private static MRJobExecutor executor;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
//		dhtConnectionProvider = Mockito.mock(IDHTConnectionProvider.class);
//		executor = MRJobExecutor.newJobExecutor(dhtConnectionProvider);
	}

	/*
	 * 
	 * List<Task> tasks = new LinkedList<Task>(job.tasks(job.currentProcedureIndex())); Task task = null; while ((task =
	 * this.taskScheduler().schedule(tasks)) != null && canExecute()) { this.dhtConnectionProvider().broadcastTaskSchedule(task);
	 * this.executeTask(task); this.dhtConnectionProvider().broadcastFinishedTask(task); } if (!canExecute()) { System.err.println(
	 * "Cannot execute! use MRJobSubmitter::canExecute(true) to enable execution"); } // all tasks finished, broadcast result
	 * this.dhtConnectionProvider().broadcastFinishedAllTasks(job);
	 * 
	 */

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() {
		
		executor.start(); 
 	}

}
