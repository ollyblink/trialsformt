package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;

/**
 * Any message updating a job should extend this class
 * 
 * @author ozihler
 *
 */
public abstract class AbstractJobBCMessage extends AbstractBCMessage implements IJobBCMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5982534238858537033L;
	protected Job job;

	public Job job() {
		return this.job;
	}

	public AbstractJobBCMessage job(Job job) {
		this.job = job;
		return this;
	}

	@Override
	public String toString() {
		return sender + " sent job message with status " + status() + " for job " + job.id();
	}

	@Override
	public BCMessageStatus status() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(IMessageConsumer messageConsumer) {
		// TODO Auto-generated method stub

	}

	@Override
	public String jobId() {
		return job.id();
	}
}
