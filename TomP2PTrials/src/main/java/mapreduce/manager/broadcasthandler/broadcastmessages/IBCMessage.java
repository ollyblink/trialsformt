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

	/**
	 * determines if this message was already processed
	 * 
	 * @return
	 */
	public boolean isAlreadyProcessed();

	/**
	 * Setter to determine if this message was already processed
	 * 
	 * @param isAlreadyProcessed
	 * @return instance of this message
	 */
	public IBCMessage isAlreadyProcessed(final boolean isAlreadyProcessed);

	public IBCMessage sender(final PeerAddress sender);

	public PeerAddress sender();

	public void execute(final IMessageConsumer messageConsumer);

}
