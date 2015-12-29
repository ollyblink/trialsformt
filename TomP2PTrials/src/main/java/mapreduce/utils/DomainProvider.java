package mapreduce.utils;

import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;

public enum DomainProvider {
	INSTANCE;
	public static final String PROCEDURE_KEYS = "PROCEDURE_KEYS";
	public static final String TASK_KEYS = "TASK_KEYS";

	public String executorTaskDomain(Tuple<String, Tuple<String, Integer>> executorTaskDomain) {
		return "EXECUTOR_TASK_DOMAIN[TASK(" + executorTaskDomain.first() + ")_PRODUCER(" + executorTaskDomain.second().first() + ")_JOB_STATUS_INDEX("
				+ executorTaskDomain.second().second() + ")]";
	}

	// Job procedure domain key generation
	public String jobProcedureDomain(Tuple<String, Tuple<String, Integer>> jobProcedureDomain) {
		return "JOB_PROCEDURE_DOMAIN[JOB(" + jobProcedureDomain.first() + ")_PROCEDURE(" + jobProcedureDomain.second().first().toUpperCase()
				+ ")_PROCEDURE_INDEX(" + jobProcedureDomain.second().second() + ")]";
	}

	// End Job procedure domain key generation

	public String concatenation(Tuple<String, Tuple<String, Integer>> jobProcedureDomain, Tuple<String, Tuple<String, Integer>> executorTaskDomain) {
		return "CONCATENATION{"+jobProcedureDomain(jobProcedureDomain) + "}:::{" + executorTaskDomain(executorTaskDomain)+"}";
	}

	public static void main(String[] args) {
		Job job = Job.create("TEST").addSubsequentProcedure(WordCountMapper.create());
		Task task = Task.create("hello", job.currentProcedure().jobProcedureDomain()).addFinalExecutorTaskDomainPart(Tuple.create("Executor1", 0));
		System.err.println("Current jobProcedureDomain: " + DomainProvider.INSTANCE.jobProcedureDomain(job.currentProcedure().jobProcedureDomain()));
		System.err.println(
				"Subsequent jobProcedureDomain: " + DomainProvider.INSTANCE.jobProcedureDomain(job.subsequentProcedure().jobProcedureDomain()));
		Tuple<String, Tuple<String, Integer>> executorTaskDomain = task.executorTaskDomain(Tuple.create("Executor1", 0));
		if (executorTaskDomain != null) {
			System.err.println("executorTaskDomain: " + DomainProvider.INSTANCE.executorTaskDomain(executorTaskDomain));
			System.err.println(
					"concatenation: " + DomainProvider.INSTANCE.concatenation(job.currentProcedure().jobProcedureDomain(), executorTaskDomain));
		}
	}

}
