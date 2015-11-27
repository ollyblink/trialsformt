package mapreduce.execution.broadcasthandler.broadcastmessages;

import java.util.Collection;

import mapreduce.execution.broadcasthandler.AbstractMessageConsumer;
import mapreduce.execution.jobtask.Task;
import net.tomp2p.peers.PeerAddress;

public class FinishedAllTasksBCMessage extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6148158172287886702L;
	private String jobId;
	private Collection<Task> tasks;

	@Override
	public JobStatus status() {
		return JobStatus.FINISHED_ALL_TASKS;
	}

	@Override
	public void execute(AbstractMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedTasks(jobId, tasks);
	}

	public FinishedAllTasksBCMessage jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	public static FinishedAllTasksBCMessage newFinishedAllTasksBCMessage() {
		return new FinishedAllTasksBCMessage();
	}

	@Override
	public FinishedAllTasksBCMessage sender(PeerAddress peerAddress) {
		return (FinishedAllTasksBCMessage) super.sender(peerAddress);
	}

	public FinishedAllTasksBCMessage tasks(Collection<Task> tasks) {
		this.tasks = tasks;
		return this;
	}

	public Collection<Task> tasks() {
		return this.tasks;
	}
}
