package mapreduce.execution.broadcasthandler.broadcastmessages;

import mapreduce.execution.broadcasthandler.AbstractMessageConsumer;
import net.tomp2p.peers.PeerAddress;

public class FinishedJobBCMessage extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7088496407737205759L;
	private String jobId;
	private String jobSubmitterId;

	@Override
	public JobStatus status() {
		return JobStatus.FINISHED_JOB;
	}

	@Override
	public void execute(AbstractMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedJob(jobId, jobSubmitterId);
	}

	public static FinishedJobBCMessage newFinishedJobBCMessage() {
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
}
