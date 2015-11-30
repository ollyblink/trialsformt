package mapreduce.execution.broadcasthandler.broadcastmessages;

import java.io.Serializable;

import mapreduce.execution.broadcasthandler.messageconsumer.IMessageConsumer;
import net.tomp2p.peers.PeerAddress;

public interface IBCMessage extends Serializable, Comparable<IBCMessage> {
	public JobStatus status();

	public void execute(IMessageConsumer messageConsumer);

	public IBCMessage sender(PeerAddress sender);

	public PeerAddress sender();

	public Long creationTime();

	public String jobId();

}
