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