package mapreduce.execution.broadcasthandler.broadcastmessages;

import mapreduce.execution.broadcasthandler.messageconsumer.IMessageConsumer;
import net.tomp2p.peers.PeerAddress;

public class FinishedJobBCMessage extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7088496407737205759L;
	private String jobSubmitterId;

	@Override
	public BCStatusType status() {
		return BCStatusType.FINISHED_JOB;
	}

	@Override
	public void execute(IMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedJob(jobId, jobSubmitterId);
	}

	public static FinishedJobBCMessage newInstance() {
		return new FinishedJobBCMessage();
	}

	public FinishedJobBCMessage jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	public FinishedJobBCMessage jobSubmitterId(String jobSubmitterId) {
		this.jobSubmitterId = jobSubmitterId;
		return this;
	}

	@Override
	public FinishedJobBCMessage sender(PeerAddress peerAddress) {
		return (FinishedJobBCMessage) super.sender(peerAddress);
	}

	@Override
	public String jobId() {
		return jobId;
	}	
}
