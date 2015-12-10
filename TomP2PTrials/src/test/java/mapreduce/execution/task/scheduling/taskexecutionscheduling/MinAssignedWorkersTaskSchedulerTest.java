package mapreduce.execution.task.scheduling.taskexecutionscheduling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;

public class MinAssignedWorkersTaskSchedulerTest {

	private static final Random RND = new Random();
	private static MinAssignedWorkersTaskExecutionScheduler taskScheduler;
	private Job job;
	private static LinkedList<Task> tasks;

	@Before
	public void setUpBeforeTest() throws Exception {
		taskScheduler = MinAssignedWorkersTaskExecutionScheduler.newInstance();

		Task[] tasksToTest = new Task[12];

		tasksToTest[0] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[0].id()).thenReturn("1");
		Mockito.when(tasksToTest[0].totalNumberOfFinishedExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[0].totalNumberOfCurrentExecutions()).thenReturn(0);
		Mockito.when(tasksToTest[0].numberOfDifferentPeersExecutingTask()).thenReturn(2);
		Mockito.when(tasksToTest[0].isFinished()).thenReturn(false);

		tasksToTest[1] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[1].id()).thenReturn("2");
		Mockito.when(tasksToTest[1].totalNumberOfFinishedExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[1].totalNumberOfCurrentExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[1].numberOfDifferentPeersExecutingTask()).thenReturn(2);
		Mockito.when(tasksToTest[1].isFinished()).thenReturn(false);

		tasksToTest[2] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[2].id()).thenReturn("3");
		Mockito.when(tasksToTest[2].totalNumberOfFinishedExecutions()).thenReturn(0);
		Mockito.when(tasksToTest[2].totalNumberOfCurrentExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[2].numberOfDifferentPeersExecutingTask()).thenReturn(2);
		Mockito.when(tasksToTest[2].isFinished()).thenReturn(false);

		tasksToTest[3] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[3].id()).thenReturn("4");
		Mockito.when(tasksToTest[3].totalNumberOfFinishedExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[3].totalNumberOfCurrentExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[3].numberOfDifferentPeersExecutingTask()).thenReturn(2);
		Mockito.when(tasksToTest[3].isFinished()).thenReturn(false);

		//
		tasksToTest[4] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[4].id()).thenReturn("5");
		Mockito.when(tasksToTest[4].totalNumberOfFinishedExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[4].totalNumberOfCurrentExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[4].numberOfDifferentPeersExecutingTask()).thenReturn(2);
		Mockito.when(tasksToTest[4].isFinished()).thenReturn(false);

		tasksToTest[5] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[5].id()).thenReturn("6");
		Mockito.when(tasksToTest[5].totalNumberOfFinishedExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[5].totalNumberOfCurrentExecutions()).thenReturn(0);
		Mockito.when(tasksToTest[5].numberOfDifferentPeersExecutingTask()).thenReturn(1);
		Mockito.when(tasksToTest[5].isFinished()).thenReturn(false);

		tasksToTest[6] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[6].id()).thenReturn("7");
		Mockito.when(tasksToTest[6].totalNumberOfFinishedExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[6].totalNumberOfCurrentExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[6].numberOfDifferentPeersExecutingTask()).thenReturn(1);
		Mockito.when(tasksToTest[6].isFinished()).thenReturn(false);

		tasksToTest[7] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[7].id()).thenReturn("8");
		Mockito.when(tasksToTest[7].totalNumberOfFinishedExecutions()).thenReturn(0);
		Mockito.when(tasksToTest[7].totalNumberOfCurrentExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[7].numberOfDifferentPeersExecutingTask()).thenReturn(1);
		Mockito.when(tasksToTest[7].isFinished()).thenReturn(false);
		//
		tasksToTest[8] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[8].id()).thenReturn("9");
		Mockito.when(tasksToTest[8].totalNumberOfFinishedExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[8].totalNumberOfCurrentExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[8].numberOfDifferentPeersExecutingTask()).thenReturn(1);
		Mockito.when(tasksToTest[8].isFinished()).thenReturn(false);

		//
		tasksToTest[9] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[9].id()).thenReturn("10");
		Mockito.when(tasksToTest[9].totalNumberOfFinishedExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[9].totalNumberOfCurrentExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[9].numberOfDifferentPeersExecutingTask()).thenReturn(1);
		Mockito.when(tasksToTest[9].isFinished()).thenReturn(false);

		tasksToTest[10] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[10].id()).thenReturn("11");
		Mockito.when(tasksToTest[10].totalNumberOfFinishedExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[10].totalNumberOfCurrentExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[10].numberOfDifferentPeersExecutingTask()).thenReturn(1);
		Mockito.when(tasksToTest[10].isFinished()).thenReturn(true);

		tasksToTest[11] = Mockito.mock(Task.class);
		Mockito.when(tasksToTest[11].id()).thenReturn("12");
		Mockito.when(tasksToTest[11].totalNumberOfFinishedExecutions()).thenReturn(1);
		Mockito.when(tasksToTest[11].totalNumberOfCurrentExecutions()).thenReturn(2);
		Mockito.when(tasksToTest[11].numberOfDifferentPeersExecutingTask()).thenReturn(1);
		Mockito.when(tasksToTest[11].isFinished()).thenReturn(true);

		tasks = new LinkedList<Task>();
		Collections.addAll(tasks, tasksToTest);
	}

	@Test
	public void testScheduledTasks() {
		// Test result should be the following order (task ids): 8,3,9,10,4,5,6,7,1,2
		// List<Task> ts = new LinkedList<Task>(tasks);
		// for(Task task: tasks){
		// Mockito.when(task.isFinished()).thenReturn(true);
		// }
		taskScheduler.randomizeFirstTask(false);
		Task task1 = taskScheduler.schedule(tasks);

		assertEquals(task1.id(), ("8"));
		assertTrue("task id 8", tasks.get(0).id().equals("8"));
		assertTrue("task id 3", tasks.get(1).id().equals("3"));
		assertTrue("task id 9", tasks.get(2).id().equals("9"));
		assertTrue("task id 10", tasks.get(3).id().equals("10"));
		assertTrue("task id 4", tasks.get(4).id().equals("4"));
		assertTrue("task id 5", tasks.get(5).id().equals("5"));
		assertTrue("task id 6", tasks.get(6).id().equals("6"));
		assertTrue("task id 7", tasks.get(7).id().equals("7"));
		assertTrue("task id 1", tasks.get(8).id().equals("1"));
		assertTrue("task id 2", tasks.get(9).id().equals("2"));
		assertTrue("task id 12", tasks.get(11).id().equals("12"));
		assertTrue("task id 11", tasks.get(10).id().equals("11"));
	}

	@Test
	public void testRandomised() {
		taskScheduler.randomizeFirstTask(true);

		/*
		 * Let's count some occurrences... If it is uniformly distributed, the larger the iterations, the more the proportions should come to a ratio
		 * of 1/#tasks
		 */
		tasks.removeLast(); //Problem: when its finished, it will not be chosen..
		tasks.removeLast();
		Map<Task, Double> taskOccurrenceCounter = new HashMap<Task, Double>();
		for (Task task : tasks) {
			taskOccurrenceCounter.put(task, 0.0);
		}
		int numberOfIterations = 10000;
		for (int i = 0; i < numberOfIterations; ++i) {
			Task task = taskScheduler.schedule(tasks);
			Double taskCounter = taskOccurrenceCounter.get(task);
			++taskCounter;
			taskOccurrenceCounter.put(task, taskCounter);
		}

		for (Task task : taskOccurrenceCounter.keySet()) {
			System.err.println((1.0 / ((double) tasks.size())) + "," + (((double) taskOccurrenceCounter.get(task)) / ((double) numberOfIterations)));
			assertEquals((1.0 / ((double) tasks.size())), (((double) taskOccurrenceCounter.get(task)) / ((double) numberOfIterations)), 0.01);
		}
		taskScheduler.randomizeFirstTask(false);
	}
}