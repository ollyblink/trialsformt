package mapreduce.manager.broadcasting.broadcastmessages.jobmessages;

import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.IMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;
import net.tomp2p.peers.Number160;

public class TaskCompletedBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7388714464226222965L;
	protected Task task;
	protected BCMessageStatus status;
	private Number160 resultHash;

	@Override
	public BCMessageStatus status() {
		return status;
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		TaskResult taskResult = TaskResult.create().sender(sender).status(status).resultHash(resultHash);
		messageConsumer.handleTaskExecutionStatusUpdate(job, task, taskResult);

	}

	public static TaskCompletedBCMessage createTaskFinishedBCMessage() {
		return new TaskCompletedBCMessage(BCMessageStatus.FINISHED_TASK);
	}

	private TaskCompletedBCMessage(BCMessageStatus status) {
		this.status = status;
	}

	public Number160 resultHash() {
		return this.resultHash;
	}

	public TaskCompletedBCMessage resultHash(Number160 resultHash) {
		this.resultHash = resultHash;
		return this;
	}

	@Override
	public TaskCompletedBCMessage sender(final String sender) {
		return (TaskCompletedBCMessage) super.sender(sender);
	}

	public TaskCompletedBCMessage task(Task task) {
		this.task = task;
		return this;
	}

	public Task task() {
		return task;
	}
}
