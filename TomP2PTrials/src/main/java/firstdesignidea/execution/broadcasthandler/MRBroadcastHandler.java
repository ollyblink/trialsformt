package firstdesignidea.execution.broadcasthandler;

import firstdesignidea.execution.jobtask.IJobManager;
import firstdesignidea.execution.jobtask.JobStatus;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number160;

public class MRBroadcastHandler extends StructuredBroadcastHandler {

	private IJobManager jobManager;

	public MRBroadcastHandler(IJobManager jobManager) {
		this.jobManager = jobManager;
	}

	public MRBroadcastHandler() {

	}

	public MRBroadcastHandler jobManager(IJobManager jobManager) {
		this.jobManager = jobManager;
		return this;
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		super.receive(message);

		JobStatus jobStatus = JobStatus.DISTRIBUTED_TASKS; // to get from message
 
		switch (jobStatus) { 
		case DISTRIBUTED_TASKS: 
			break;
		case FINISHED_TASKS: 
			break; 
		default:
			break;
		}  
		return this;
	}

}
