package mapreduce.manager.broadcasthandler.messageconsumer;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.storage.DHTConnectionProvider;

public class MRJobExecutorMessageConsumer_MRBroadcastHandler_Interaction_Test {

	private static MRJobExecutionManagerMessageConsumer messageConsumer;
	private static DHTConnectionProvider dht;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		messageConsumer = MRJobExecutionManagerMessageConsumer.newInstance(new CopyOnWriteArrayList<Job>()).canTake(true);
		// new Thread(messageConsumer).start();
		//
		// dht = DHTConnectionProvider.newDHTConnectionProvider().port(4000).connect();
		// dht.broadcastHandler().broadcastListener().queue(messageConsumer.queue());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// dht.shutdown();

	}

	// @Test
	// public void testCancelJob() throws IOException, InterruptedException {
	// Job job = Job.newJob("ME");
	// dht.broadcastNewJob(job);
	// dht.broadcastNewJob(job);
	// dht.broadcastNewJob(job);
	// dht.broadcastNewJob(job);
	// Thread.sleep(Long.MAX_VALUE);
	// }

	@Test
	public void testManagers() { 

	}

}
