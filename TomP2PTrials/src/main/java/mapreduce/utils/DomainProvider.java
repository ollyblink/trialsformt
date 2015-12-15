package mapreduce.utils;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import net.tomp2p.peers.PeerAddress;

public enum DomainProvider {
	INSTANCE;

	public String executorTaskDomain(Task task) {
		return executorTaskDomain(task.id(), task.finalDataLocation().first().peerId().toString(), task.finalDataLocation().second());
	}

	public String executorTaskDomain(String taskId, String peerId, Integer jobStatusIndex) {
		return taskId + "_PRODUCER_PEER_ID_" + peerId + "_JOB_STATUS_INDEX_" + jobStatusIndex;
	}
 

	// Job procedure domain key generation
	public String jobProcedureDomain(String jobId, String procedureSimpleName, Integer procedureIndex) {
		return jobId + "_PROCEDURE_" + procedureSimpleName.toUpperCase() + "_" + procedureIndex;
	}

	public String jobProcedureDomain(Job job) {
		return jobProcedureDomain(job.id(), job.procedure(job.currentProcedureIndex()).getClass().getSimpleName(), job.currentProcedureIndex());
	}
	// End Job procedure domain key generation

}
