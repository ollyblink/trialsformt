package mapreduce.manager.broadcasthandler.broadcastmessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;
import net.tomp2p.peers.PeerAddress;

public class DistributedJobBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2792435155108555907L;

	private DistributedJobBCMessage() {
	}

	public static DistributedJobBCMessage newInstance() {
		return new DistributedJobBCMessage();
	}

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.DISTRIBUTED_JOB;
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleReceivedJob(job);

	} 

}
