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

		JobStatus jobStatus = JobStatus.NEW_JOB_AVAILABLE; // to get from message
 
		switch (jobStatus) {
		case NEW_JOB_AVAILABLE:

			Number160 jobQueueLocation = Number160.createHash("Job Queue Location"); //to get from message
			jobManager.scheduleJobs(jobQueueLocation);
			break;
		case DISTRIBUTED_MAP_TASKS:
			Number160 jobLocation =  Number160.createHash("jobId1"); //to get from message
			jobManager.startMapping(jobLocation);
			break;
		case FINISHED_MAP_TASKS:
			jobManager.scheduleReduceTasks();
			break;
		case DISTRIBUTED_REDUCE_TASKS:
			jobManager.startReducing();
			break;
		default:
			break;
		}  
		return this;
	}

}
