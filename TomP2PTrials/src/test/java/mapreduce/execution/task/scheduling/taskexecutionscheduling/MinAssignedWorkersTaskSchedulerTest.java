package mapreduce.execution.task.scheduling.taskexecutionscheduling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MinAssignedWorkersTaskSchedulerTest {

	private static MinAssignedWorkersTaskExecutionScheduler taskScheduler;

	private static LinkedList<Task> tasks;

	@Before
	public void setUpBeforeTest() throws Exception {
		taskScheduler = MinAssignedWorkersTaskExecutionScheduler.newInstance();

		tasks = new LinkedList<Task>();

		Task task = Task.newInstance("1", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(2)), BCMessageStatus.FINISHED_TASK);
		task.isFinished(false);
		tasks.add(task);

		task = Task.newInstance("2", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(2)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(2)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(false);
		tasks.add(task);

		task = Task.newInstance("3", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(2)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(false);
		tasks.add(task);

		task = Task.newInstance("4", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(2)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(false);
		tasks.add(task);

		task = Task.newInstance("5", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(2)), BCMessageStatus.EXECUTING_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(2)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(false);
		tasks.add(task);

		task = Task.newInstance("6", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.isFinished(false);
		tasks.add(task);

		task = Task.newInstance("7", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(false);
		tasks.add(task);

		task = Task.newInstance("8", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(false);
		tasks.add(task);

		task = Task.newInstance("9", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(false);
		tasks.add(task);
		//
		task = Task.newInstance("10", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(false);
		tasks.add(task);

		task = Task.newInstance("11", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(true);
		tasks.add(task);

		task = Task.newInstance("12", "0");
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.FINISHED_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.executingPeers().put(new PeerAddress(new Number160(1)), BCMessageStatus.EXECUTING_TASK);
		task.isFinished(true);
		tasks.add(task);
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

		Map<Task, Double> taskOccurrenceCounter = new HashMap<Task, Double>();
		for (Task task : tasks) {
			task.executingPeers().clear();
			task.isFinished(false);
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

	@Ignore //Redundant
	public void waitForDataFetch() {
		// consumerT1 should be able to fetch the data since it waits longer for it to be created than consumerT2

		List<Task> tasks2 = Collections.synchronizedList(new ArrayList<>());

		Thread consumerT1 = new Thread(new Runnable() {

			@Override
			public void run() {
				assertEquals(Task.newInstance("1", "0"), taskScheduler.schedule(tasks2));
			}

		});
		Thread consumerT2 = new Thread(new Runnable() {

			@Override
			public void run() {
				assertEquals(null, taskScheduler.schedule(tasks2));
			}

		});
		Thread producer = new Thread(new Runnable() {

			@Override
			public void run() {
				// waits 5 seconds before creating the item
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				tasks2.add(Task.newInstance("1", "0"));

			}

		});

		consumerT1.start();
		consumerT2.start();
		producer.start();
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
