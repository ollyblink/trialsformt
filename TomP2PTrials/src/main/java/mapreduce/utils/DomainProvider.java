package mapreduce.utils;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;

public enum DomainProvider {
	INSTANCE;
	public static final String PROCEDURE_OUTPUT_RESULT_KEYS = "PROCEDURE_OUTPUT_RESULT_KEYS";
	public static final String TASK_OUTPUT_RESULT_KEYS = "TASK_OUTPUT_RESULT_KEYS";
	public static final String JOB = "JOB";

	public String executorTaskDomain(ExecutorTaskDomain executorTaskDomainParameter) {
		// ETD = EXECUTOR_TASK_DOMAIN
		// T = taskId
		// E = taskExecutor
		// TSI = taskStatusIndex
		// S = taskSubmissionCount
		// C = taskCreationTime
		return "ETD[T(" + executorTaskDomainParameter.taskId() + ")_P(" + executorTaskDomainParameter.executor() + ")_JSI("
				+ executorTaskDomainParameter.taskStatusIndex() + ")_S(" + executorTaskDomainParameter.submissionCount() + ")_C("
				+ executorTaskDomainParameter.creationTime() + ")]";
	}

	// Job procedure domain key generation
	public String jobProcedureDomain(JobProcedureDomain jobProcedureDomainParameter) {
		// JPD = JOB_PROCEDURE_DOMAIN
		// J = jobId
		// E = procedureExecutor
		// P = procedureSimpleName
		// PI = procedureIndex
		// S = procedureSubmissionCount
		// C = procedureCreationTime
		return "JPD[J(" + jobProcedureDomainParameter.jobId() + ")_E(" + jobProcedureDomainParameter.executor() + ")_P("
				+ jobProcedureDomainParameter.procedureSimpleName().toUpperCase() + 
				")_PI(" + jobProcedureDomainParameter.procedureIndex() + ")_S("
				+ jobProcedureDomainParameter.submissionCount() + ")_C(" + jobProcedureDomainParameter.creationTime() + ")]";
	}

	// End Job procedure domain key generation

	public String concatenation(JobProcedureDomain jobProcedureDomainParameter, ExecutorTaskDomain executorTaskDomainParameter) {
		// C = CONCATENATION
		return "C{" + jobProcedureDomain(jobProcedureDomainParameter) + "}:::{" + executorTaskDomain(executorTaskDomainParameter) + "}";
	}

	public static void main(String[] args) {

	}

}
