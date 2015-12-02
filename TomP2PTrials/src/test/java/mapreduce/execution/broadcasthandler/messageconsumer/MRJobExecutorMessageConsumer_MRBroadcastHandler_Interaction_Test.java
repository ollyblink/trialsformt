package mapreduce.execution.broadcasthandler.messageconsumer;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.jobtask.Job;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobExecutorMessageConsumer;
import mapreduce.manager.broadcasthandler.broadcastmessages.DistributedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.FinishedAllTasksBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.DHTConnectionProviderTest;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class MRJobExecutorMessageConsumer_MRBroadcastHandler_Interaction_Test {

	private static MRJobExecutorMessageConsumer messageConsumer;
	private static DHTConnectionProvider dht;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		messageConsumer = MRJobExecutorMessageConsumer.newInstance(new LinkedBlockingQueue<Job>()).canTake(true);
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
		Job job = Job.newInstance("ME");
		messageConsumer.handleBCMessage(DistributedJobBCMessage.newInstance().job(job).sender(new PeerAddress(new Number160(1))));
		messageConsumer.handleBCMessage(DistributedJobBCMessage.newInstance().job(job).sender(new PeerAddress(new Number160(2))));
		messageConsumer.handleBCMessage(DistributedJobBCMessage.newInstance().job(job).sender(new PeerAddress(new Number160(3))));
		messageConsumer.handleBCMessage(DistributedJobBCMessage.newInstance().job(job).sender(new PeerAddress(new Number160(4))));

	}

}
