package mapreduce.manager.broadcasthandler.broadcastmessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.utils.IDCreator;

public abstract class AbstractBCMessage implements IBCMessage {
	protected static Logger logger = LoggerFactory.getLogger(AbstractBCMessage.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -2040511707608747442L;
	protected String id;
	protected String sender;
	protected Long creationTime = System.currentTimeMillis();

	private boolean isAlreadyProcessed;;

	@Override
	public AbstractBCMessage sender(final String sender) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.sender = sender;
		return this;
	}

	@Override
	public String sender() {
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

	 

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sender == null) ? 0 : sender.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AbstractBCMessage other = (AbstractBCMessage) obj;
		if (sender == null) {
			if (other.sender != null)
				return false;
		} else if (!sender.equals(other.sender))
			return false;
		return true;
	}

	@Override
	public String id() {
		return this.id;
	}

	@Override
	public String toString() {
		return "AbstractBCMessage [id=" + id + ", sender=" + sender + ", creationTime=" + creationTime + ", sender()=" + sender()
				+ ", creationTime()=" + creationTime() + ", isAlreadyProcessed()=" + isAlreadyProcessed() + ", id()=" + id() + "]\n";
	}

}