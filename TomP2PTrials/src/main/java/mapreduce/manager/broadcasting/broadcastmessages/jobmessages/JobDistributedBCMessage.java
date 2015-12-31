package mapreduce.manager.broadcasting.broadcastmessages.jobmessages;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.IMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;

public class JobDistributedBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2792435155108555907L;

	private JobDistributedBCMessage() {
	}

	public static JobDistributedBCMessage newInstance() {
		return new JobDistributedBCMessage();
	}

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.DISTRIBUTED_JOB;
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleReceivedJob(job);
	}

	@Override
	public JobDistributedBCMessage sender(final String sender) {
		super.sender(sender);
		return this;
	}

	@Override
	public JobDistributedBCMessage job(Job job) {
		super.job(job);
		return this;
	}
}
