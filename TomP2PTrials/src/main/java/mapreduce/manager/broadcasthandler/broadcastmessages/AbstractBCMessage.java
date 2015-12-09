package mapreduce.manager.broadcasthandler.broadcastmessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;
import net.tomp2p.peers.PeerAddress;

public abstract class AbstractBCMessage implements IBCMessage {
	protected static Logger logger = LoggerFactory.getLogger(AbstractBCMessage.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -2040511707608747442L;

	protected PeerAddress sender;
	protected Long creationTime = System.currentTimeMillis();

	private boolean isAlreadyProcessed;;

	@Override
	public AbstractBCMessage sender(final PeerAddress sender) {
		this.sender = sender;
		return this;
	}

	@Override
	public PeerAddress sender() {
		return sender;
	}

	@Override
	public Long creationTime() {
		return creationTime;
	}

	@Override
	public int compareTo(IBCMessage o) {
		AbstractBCMessage other = (AbstractBCMessage) o;
		if (sender != null) {
			if (sender.equals(other.sender)) {
				// The sender is the same, in that case, the messages should be sorted by creation time (preserve total ordering)
				return creationTime.compareTo(other.creationTime);
			}
		}
		// else don't bother, just make sure more important messages come before less important, such that e.g. an executing peer can be stopped if
		// enough tasks were already finished
		return status().compareTo(other.status());
	}

	@Override
	public boolean isAlreadyProcessed() {
		return this.isAlreadyProcessed;
	}

	@Override
	public IBCMessage isAlreadyProcessed(boolean isAlreadyProcessed) {
		this.isAlreadyProcessed = isAlreadyProcessed;
		return this;
	}

}