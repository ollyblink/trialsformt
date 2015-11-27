package mapreduce.execution.broadcasthandler.messageconsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.broadcasthandler.broadcastmessages.ExecuteOrFinishedTaskMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
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
	public void test() throws InterruptedException {
		Random random = new Random();
		BlockingQueue<IBCMessage> messages = new PriorityBlockingQueue<IBCMessage>();
		final List<IBCMessage> trial = new ArrayList<IBCMessage>();
		ExecutorService s = Executors.newFixedThreadPool(10);

		for (double i = 2; i < 6; i += 0.5) {
			final double in = i;
			new Thread(new Runnable() {

				@Override
				public void run() {
					if ((in) % ((int) in) == 0.5) {
						System.err.println((in) % ((int) in));
						trial.add(ExecuteOrFinishedTaskMessage.newFinishedTaskBCMessage().sender(new PeerAddress(Number160.createHash((1))))
								.jobId("1").taskId("1"));
					} else {
						System.err.println((in) % ((int) in));
						trial.add(ExecuteOrFinishedTaskMessage.newTaskAssignedBCMessage().sender(new PeerAddress(Number160.createHash((1))))
								.jobId("1").taskId("1"));
					}
				}

			}).start();

		}
		s.shutdown();
		while (!s.isTerminated()) {
			Thread.sleep(100); 
		}

		Thread.sleep(1000);

		while (!trial.isEmpty()) {
			IBCMessage m = trial.remove(random.nextInt(trial.size()));

			if (m != null) {

				messages.add(m);
			}
		}

		while (!messages.isEmpty()) {
			System.err.println(messages.peek().sender().peerId() + " " + messages.peek().creationTime() + " " + messages.poll());
		}
	}

}
