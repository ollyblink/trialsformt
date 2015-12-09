package mapreduce.execution.task.scheduling.taskresultcomparisonscheduling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MinAssignedWorkersTaskResultComparisonSchedulerTest {

	private static final Random RND = new Random();
	private static MinAssignedWorkersTaskResultComparisonScheduler taskScheduler;
	private Job job;
	private static LinkedList<Task> tasks;

	@Before
	public void setUpBeforeTest() throws Exception {
		taskScheduler = MinAssignedWorkersTaskResultComparisonScheduler.newInstance();
		PeerAddress p1 = new PeerAddress(new Number160(1));
		Task[] tasksToTest = new Task[6];

		tasksToTest[0] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[0].id()).thenReturn("1");
		Mockito.when(tasksToTest[0].finalDataLocation()).thenReturn(null);
		Mockito.when(tasksToTest[0].taskComparisonAssigned()).thenReturn(false);

		tasksToTest[1] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[1].id()).thenReturn("2");
		Mockito.when(tasksToTest[1].finalDataLocation()).thenReturn(null);
		Mockito.when(tasksToTest[1].taskComparisonAssigned()).thenReturn(false);

		tasksToTest[2] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[2].id()).thenReturn("3");
		Mockito.when(tasksToTest[2].finalDataLocation()).thenReturn(null);
		Mockito.when(tasksToTest[2].taskComparisonAssigned()).thenReturn(true);

		tasksToTest[3] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[3].id()).thenReturn("5");
		Mockito.when(tasksToTest[3].finalDataLocation()).thenReturn(Tuple.create(p1, 0));
		Mockito.when(tasksToTest[3].taskComparisonAssigned()).thenReturn(true);

		tasksToTest[4] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[4].id()).thenReturn("6");
		Mockito.when(tasksToTest[4].finalDataLocation()).thenReturn(Tuple.create(p1, 0));
		Mockito.when(tasksToTest[4].taskComparisonAssigned()).thenReturn(true);

		tasksToTest[5] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[5].id()).thenReturn("4");
		Mockito.when(tasksToTest[5].finalDataLocation()).thenReturn(null);
		Mockito.when(tasksToTest[5].taskComparisonAssigned()).thenReturn(true);

		tasks = new LinkedList<Task>();
		Collections.addAll(tasks, tasksToTest);
	}

	@Test
	public void testScheduledTasks() {
		Task task1 = taskScheduler.schedule(tasks);
		for (Task t : tasks) {
			System.err.println(t.id());
		}
		assertEquals(task1.id(), ("1"));
		assertTrue("task id 1", tasks.get(0).id().equals("1"));
		assertTrue("task id 2", tasks.get(1).id().equals("2"));
		assertTrue("task id 3", tasks.get(2).id().equals("3"));
		assertTrue("task id 4", tasks.get(3).id().equals("4"));
		assertTrue("task id 5", tasks.get(4).id().equals("5"));
		assertTrue("task id 6", tasks.get(5).id().equals("6"));
	}

	@Test
	public void testRandomised() {
		// tasks.clear();
		taskScheduler.randomizeFirstTask(true);
		/*
		 * Let's count some occurrences... If it is uniformly distributed, the larger the iterations, the more the proportions should come to a ratio
		 * of 1/#tasks
		 */
		Map<Task, Double> taskOccurrenceCounter = new HashMap<Task, Double>();
		for (int i = 0; i < 7; ++i) {
			Task task = Mockito.mock(Task.class);
			Mockito.when(task.id()).thenReturn(i + "");
			Mockito.when(task.finalDataLocation()).thenReturn(null);
			Mockito.when(task.taskComparisonAssigned()).thenReturn(false);
			taskOccurrenceCounter.put(task, new Double(0.0));
		}
		int numberOfIterations = 20000;
		for (int i = 0; i < numberOfIterations; ++i) {
			Task task = taskScheduler.schedule(new ArrayList<Task>(taskOccurrenceCounter.keySet()));
			Double taskCounter = taskOccurrenceCounter.get(task);
			++taskCounter;
			taskOccurrenceCounter.put(task, taskCounter);
		}

		for (Task task : taskOccurrenceCounter.keySet()) {
			System.err.println((1.0 / ((double) taskOccurrenceCounter.keySet().size())) + ","
					+ (((double) taskOccurrenceCounter.get(task)) / ((double) numberOfIterations)));
			assertEquals((1.0 / ((double) taskOccurrenceCounter.keySet().size())),
					(((double) taskOccurrenceCounter.get(task)) / ((double) numberOfIterations)), 0.01);
		}
		taskScheduler.randomizeFirstTask(false);
	}
}
