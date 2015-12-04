package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.execution.jobtask.Task;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;

/**
 * Any message updating a task should extend this class
 * 
 * @author ozihler
 *
 */
public abstract class AbstractTaskBCMessage extends AbstractBCMessage implements IJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5274137998974200574L;
	protected Task task;

	public AbstractTaskBCMessage task(Task task) {
		this.task = task;
		return this;
	}

	public Task task() {
		return task;
	}

	@Override
	public String toString() {
		return creationTime() + ":"+sender.inetAddress() + ":" + sender.tcpPort() + " sent task message with status " + status() + " for task " + task.id() + " from job "
				+ task.jobId()+"\n";
	}

	@Override
	public String jobId() {
		return task.jobId();
	}
}
