package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;

public class JobFailedBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3017121861722797890L;

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.JOB_FAILED;
	}

	@Override
	public void execute(IMessageConsumer messageConsumer) {
		messageConsumer.handleFailedJob(job, sender);
	}

	public static JobFailedBCMessage newInstance() {
		return new JobFailedBCMessage();
	}

}
