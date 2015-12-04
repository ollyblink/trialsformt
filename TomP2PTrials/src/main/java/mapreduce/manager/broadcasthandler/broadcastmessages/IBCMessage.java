package mapreduce.manager.broadcasthandler.broadcastmessages;

import java.io.Serializable;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;
import net.tomp2p.peers.PeerAddress;

public interface IBCMessage extends Serializable, Comparable<IBCMessage> {
	public BCMessageStatus status();

	/**
	 * @return Unix timestamp of when the message was created
	 */
	public Long creationTime();

	public IBCMessage sender(final PeerAddress sender);

	public PeerAddress sender();

	public void execute(final IMessageConsumer messageConsumer);

}
