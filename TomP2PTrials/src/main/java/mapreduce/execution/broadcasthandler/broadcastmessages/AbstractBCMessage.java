package mapreduce.execution.broadcasthandler.broadcastmessages;

public abstract class AbstractBCMessage implements IBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2040511707608747442L;

	@Override
	public int compareTo(IBCMessage o) {
		return status().compareTo(o.status());
	}
}