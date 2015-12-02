package mapreduce.manager.broadcasthandler.broadcastmessages;

import java.io.Serializable;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;
import net.tomp2p.peers.PeerAddress;

public interface IBCMessage extends Serializable, Comparable<IBCMessage> {
	public BCStatusType status();

	public void execute(IMessageConsumer messageConsumer);

	public IBCMessage sender(PeerAddress sender);

	public PeerAddress sender();

	public Long creationTime();

	public String jobId();

}
