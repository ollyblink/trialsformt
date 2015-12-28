package mapreduce.utils;

public enum DomainProvider {
	INSTANCE;
	public static final String PROCEDURE_KEYS = "PROCEDURE_KEYS";
	public static final String TASK_KEYS = "TASK_KEYS";

	public String executorTaskDomain(String taskId, String producer, Integer jobStatusIndex) {
		return taskId + "_PRODUCER_" + producer + "_JOB_STATUS_INDEX_" + jobStatusIndex;
	}

	// Job procedure domain key generation
	public String jobProcedureDomain(String jobId, String procedureSimpleName, Integer procedureIndex, Integer submissionNr) {
		return jobId + "_PROCEDURE_" + procedureSimpleName.toUpperCase() + "_" + procedureIndex + "_" + submissionNr;
	}

	// End Job procedure domain key generation

	public String concatenation(String jobId, String procedureSimpleName, Integer procedureIndex, Integer submissionNr, String taskId,
			String producer, Integer jobStatusIndex) {
		return jobProcedureDomain(jobId, procedureSimpleName, procedureIndex, submissionNr) + "_"
				+ executorTaskDomain(taskId, producer, jobStatusIndex);
	}

}
