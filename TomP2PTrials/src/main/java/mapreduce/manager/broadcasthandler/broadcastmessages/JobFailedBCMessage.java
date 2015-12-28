package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;

public class JobFailedBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3017121861722797890L;

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.FAILED_JOB;
	}

	@Override
	public void execute(IMessageConsumer messageConsumer) {
		messageConsumer.handleFailedJob(job);
	}

	public static JobFailedBCMessage newInstance() {
		return new JobFailedBCMessage();
	}

	@Override
	public JobFailedBCMessage sender(final String sender) {
		return (JobFailedBCMessage) super.sender(sender);
	}

	@Override
	public JobFailedBCMessage job(Job job) {
		super.job(job);
		return this;
	}
}
