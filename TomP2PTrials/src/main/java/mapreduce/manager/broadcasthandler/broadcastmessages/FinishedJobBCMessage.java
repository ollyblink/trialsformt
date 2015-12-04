package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;
import net.tomp2p.peers.PeerAddress;

public class FinishedJobBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7088496407737205759L;
	private String jobSubmitterId;

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.FINISHED_JOB;
	}

	public static FinishedJobBCMessage newInstance() {
		return new FinishedJobBCMessage();
	}

	public FinishedJobBCMessage jobSubmitterId(String jobSubmitterId) {
		this.jobSubmitterId = jobSubmitterId;
		return this;
	}


	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedJob(job, jobSubmitterId);
	}
 
}
