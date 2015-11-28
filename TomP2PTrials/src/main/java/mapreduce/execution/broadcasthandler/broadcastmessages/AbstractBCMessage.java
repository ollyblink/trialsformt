package mapreduce.execution.broadcasthandler.broadcastmessages;

import net.tomp2p.peers.PeerAddress;

public abstract class AbstractBCMessage implements IBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2040511707608747442L;
	protected PeerAddress sender;
	private Long creationTime = System.currentTimeMillis();;

	@Override
	public int compareTo(IBCMessage o) {
		if (sender != null) {
			if (sender.equals(o.sender())) {
				// The sender is the same, in that case, the messages should be sorted by creation time (preserve total ordering)
				return creationTime.compareTo(o.creationTime());
			}
		}
		// else don't bother, just make sure more important messages come before less important, such that e.g. an executing peer can be stopped if
		// enough tasks were already finished
		return status().compareTo(o.status());
	}

	@Override
	public AbstractBCMessage sender(PeerAddress sender) {
		this.sender = sender;
		return this;
	}

	@Override
	public PeerAddress sender() {
		return this.sender;
	}

	@Override
	public Long creationTime() {
		return this.creationTime;
	}

}