package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;

public class FinishedJobBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7088496407737205759L;

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.FINISHED_JOB;
	}

	public static FinishedJobBCMessage newInstance() {
		return new FinishedJobBCMessage();
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedJob(job);
	}

}
