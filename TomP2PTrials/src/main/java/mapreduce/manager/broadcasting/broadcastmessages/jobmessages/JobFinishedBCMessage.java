package mapreduce.manager.broadcasting.broadcastmessages.jobmessages;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.IMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;

public class JobFinishedBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7088496407737205759L;

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.FINISHED_JOB;
	}

	public static JobFinishedBCMessage newInstance() {
		return new JobFinishedBCMessage();
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedJob(job);
	}
	@Override
	public JobFinishedBCMessage sender(final String sender) {
		return (JobFinishedBCMessage) super.sender(sender);
	}

	@Override
	public JobFinishedBCMessage job(Job job) {
		super.job(job);
		return this;
	}
}
