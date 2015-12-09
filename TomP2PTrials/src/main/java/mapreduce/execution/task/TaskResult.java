package mapreduce.execution.task;

import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class TaskResult {
	PeerAddress sender;
	BCMessageStatus status;
	Number160 resultHash;

	public PeerAddress sender() {
		return sender;
	}

	public TaskResult sender(PeerAddress sender) {
		this.sender = sender;
		return this;
	}

	public BCMessageStatus status() {
		return status;
	}

	public TaskResult status(BCMessageStatus status) {
		this.status = status;
		return this;
	}

	public Number160 resultHash() {
		return resultHash;
	}

	public TaskResult resultHash(Number160 resultHash) {
		this.resultHash = resultHash;
		return this;
	}

	public static TaskResult newInstance() { 
		return new TaskResult();
	}

	private TaskResult() {

	}

	@Override
	public String toString() {
		return "TaskResult [sender=" + sender.peerId() + ", status=" + status + ", resultHash=" + resultHash + "]";
	}

}
