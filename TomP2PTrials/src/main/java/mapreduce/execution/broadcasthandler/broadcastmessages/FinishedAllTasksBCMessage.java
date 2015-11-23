package mapreduce.execution.broadcasthandler.broadcastmessages;

import mapreduce.execution.broadcasthandler.MessageConsumer;
import mapreduce.server.MRJobExecutor;
import net.tomp2p.peers.PeerAddress;

public class FinishedAllTasksBCMessage  extends AbstractBCMessage{
 

	/**
	 * 
	 */
	private static final long serialVersionUID = 6148158172287886702L;
	private String jobId;

	@Override
	public JobStatus status() {
		return JobStatus.FINISHED_ALL_TASKS;
	}

	@Override
	public void execute(MessageConsumer messageConsumer) {
		messageConsumer.createNewTasksForNextProcedure(jobId);
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
		return (FinishedAllTasksBCMessage)super.sender(peerAddress); 
	}
}
