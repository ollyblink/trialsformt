package mapreduce.utils;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import net.tomp2p.peers.PeerAddress;

public enum DomainProvider {
	INSTANCE;

	public String executorTaskDomain(String taskId, String peerId, Integer jobStatusIndex) {
		return taskId + "_PRODUCER_PEER_ID_" + peerId + "_JOB_STATUS_INDEX_" + jobStatusIndex;
	}

	// Job procedure domain key generation
	public String jobProcedureDomain(String jobId, String procedureSimpleName, Integer procedureIndex, Integer submissionNr) {
		return jobId + "_PROCEDURE_" + procedureSimpleName.toUpperCase() + "_" + procedureIndex + "_" + submissionNr;
	}

	public String jobProcedureDomain(Job job) {
		ProcedureInformation procedureInformation = job.procedure(job.currentProcedureIndex());
		return jobProcedureDomain(job.id(), procedureInformation.procedure().getClass().getSimpleName(), job.currentProcedureIndex(),
				job.submissionCounter());
	}
	// End Job procedure domain key generation

	public String executorTaskDomain(Task task, Tuple<PeerAddress, Integer> domainInfo) {
		return executorTaskDomain(task.id(), domainInfo.first().peerId().toString(), domainInfo.second());
	}

}
