package mapreduce.utils;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import net.tomp2p.peers.PeerAddress;

public enum DomainProvider {
	INSTANCE;

	// Task domain key generation
	public String taskPeerDomain(Task task, PeerAddress peerAddress) {
		int jobStatusIndex = task.statiForPeer(peerAddress).size() - 1;
		if (jobStatusIndex == -1 && task.initialDataLocation() != null) { // means there's none assigned for that peer... either a problem, or take
																			// initial data location instead
			jobStatusIndex = task.initialDataLocation().second();
		}

		return this.taskPeerDomain(task, peerAddress, jobStatusIndex);
	}

	public String taskPeerDomain(final Task task, PeerAddress peerAddress, Integer jobStatusIndex) {
		String taskPeerDomain = jobProcedureDomain(task.jobId(), task.procedure().getClass().getSimpleName(), task.procedureIndex()) + "_"
				+ taskPeerDomain(task.id(), peerAddress.peerId().toString(), jobStatusIndex);
		return taskPeerDomain;
	}

	public String taskPeerDomain(String taskId, String peerId, Integer jobStatusIndex) {
		return taskId + "_PRODUCER_PEER_ID_" + peerId + "_JOB_STATUS_INDEX_" + jobStatusIndex;
	}

	// End task domain key generation
	
	// Job procedure domain key generation
	public String jobProcedureDomain(String jobId, String procedureSimpleName, Integer procedureIndex) {
		return jobId + "_PROCEDURE_" + procedureSimpleName.toUpperCase() + "_" + procedureIndex;
	}

	public String jobProcedureDomain(Job job) {
		return jobProcedureDomain(job.id(), job.procedure(job.currentProcedureIndex()).getClass().getSimpleName(), job.currentProcedureIndex());
	}
	// End Job procedure domain key generation
}
