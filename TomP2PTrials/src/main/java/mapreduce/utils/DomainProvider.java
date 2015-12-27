package mapreduce.utils;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;

public enum DomainProvider {
	INSTANCE;
	public static final String PROCEDURE_KEYS = "PROCEDURE_KEYS";

	public String executorTaskDomain(String taskId, String producer, Integer jobStatusIndex) {
		return taskId + "_PRODUCER_" + producer + "_JOB_STATUS_INDEX_" + jobStatusIndex;
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

	public String executorTaskDomain(Task task, Tuple<String, Integer> domainInfo) {
		return executorTaskDomain(task.id(), domainInfo.first(), domainInfo.second());
	}

	public String concatenation(ProcedureInformation info, Task task, Tuple<String, Integer> domainInfo) {
		return info.jobProcedureDomain() + "_" + executorTaskDomain(task, domainInfo);
	}

}
