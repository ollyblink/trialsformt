package mapreduce.execution.broadcasthandler.broadcastmessages;

import java.io.Serializable;

import mapreduce.execution.broadcasthandler.MessageConsumer;
import net.tomp2p.peers.PeerAddress;

public interface IBCMessage extends Serializable, Comparable<IBCMessage> {
	public JobStatus status();

	public void execute(MessageConsumer messageConsumer);

	public IBCMessage sender(PeerAddress sender);

	public PeerAddress sender();

}
