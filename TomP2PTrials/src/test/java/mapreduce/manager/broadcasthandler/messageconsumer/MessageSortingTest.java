package mapreduce.manager.broadcasthandler.messageconsumer;

import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MessageSortingTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testSameSender() throws InterruptedException {
		final Random random = new Random();
		final BlockingQueue<IBCMessage> messages = new PriorityBlockingQueue<IBCMessage>();
		ExecutorService s = Executors.newFixedThreadPool(10);
		final Task task = Mockito.mock(Task.class);
		Mockito.when(task.id()).thenReturn("TEST_TASK_TEST_JOB");
		Mockito.when(task.jobId()).thenReturn("TEST_JOB");
		final String sender = "EXECUTOR_1";

		for (double i = 2; i < 6; i += 0.5) {
			final double in = i;
			new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						Thread.sleep(random.nextInt(1000));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if ((in) % ((int) in) == 0.5) {
						messages.add(TaskUpdateBCMessage.newFinishedTaskInstance().task(task).sender(sender));
					} else {
						messages.add(TaskUpdateBCMessage.newExecutingTaskInstance().task(task).sender(sender));
					}
				}

			}).start();

		}
		s.shutdown();
		while (!s.isTerminated()) {
			Thread.sleep(100);
		}

		Thread.sleep(1000);
		// System.err.println("Messages: " + messages);
		IBCMessage first = messages.poll();
		while (!messages.isEmpty()) {
			IBCMessage second = messages.poll();
			System.err.println(first.creationTime());
			System.err.println(second.creationTime());
			assertTrue(first.creationTime() <= second.creationTime());
			first = second;
		}
	}

	@Test
	public void testDifferentSender() throws InterruptedException {
		final Random random = new Random();
		final BlockingQueue<IBCMessage> messages = new PriorityBlockingQueue<IBCMessage>();
		ExecutorService s = Executors.newFixedThreadPool(10);
		final Task task = Mockito.mock(Task.class);
		Mockito.when(task.id()).thenReturn("TEST_TASK_TEST_JOB");
		Mockito.when(task.jobId()).thenReturn("TEST_JOB");
		int cnt = 0;
		for (double i = 2; i < 6; i += 0.5) {
			final double in = i;
			final int inCnt = cnt++;
			new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						Thread.sleep(random.nextInt(1000));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if ((in) % ((int) in) == 0.5) {
						messages.add(TaskUpdateBCMessage.newFinishedTaskInstance().task(task).sender("EXECUTOR_"+inCnt));
					} else {
						messages.add(TaskUpdateBCMessage.newExecutingTaskInstance().task(task).sender("EXECUTOR_" + inCnt));
					}
				}

			}).start();

		}
		s.shutdown();
		while (!s.isTerminated()) {
			Thread.sleep(100);
		}

		Thread.sleep(1000);
		// System.err.println("Messages: " + messages);
		IBCMessage first = messages.poll();
		while (!messages.isEmpty()) {
			IBCMessage second = messages.poll();
			System.err.println(first.status());
			System.err.println(second.status());
			assertTrue(first.status().compareTo(second.status()) <= 0);
			first = second;
		}
	}
}
