package mapreduce.utils;

import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;

public enum DomainProvider {
	INSTANCE;
	public static final String PROCEDURE_KEYS = "PROCEDURE_KEYS";
	public static final String TASK_KEYS = "TASK_KEYS";
	public static final String JOB = "JOB";

	public String executorTaskDomain(Tuple<String, Tuple<String, Integer>> executorTaskDomain) {
		// ETD = EXECUTOR_TASK_DOMAIN
		// T = TASK
		// P = PRODUCER
		// JSI = JOB_STATUS_INDEX
		return "ETD[T(" + executorTaskDomain.first() + ")_P(" + executorTaskDomain.second().first() + ")_JSI(" + executorTaskDomain.second().second()
				+ ")]";
	}

	// Job procedure domain key generation
	public String jobProcedureDomain(Tuple<String, Tuple<String, Integer>> jobProcedureDomain) {
		// JPD = JOB_PROCEDURE_DOMAIN
		// J = JOB
		// P = PROCEDURE
		// PI = PROCEDURE_INDEX
		return "JPD[J(" + jobProcedureDomain.first() + ")_P(" + jobProcedureDomain.second().first().toUpperCase() + ")_PI("
				+ jobProcedureDomain.second().second() + ")]";
	}

	// End Job procedure domain key generation

	public String concatenation(Tuple<String, Tuple<String, Integer>> jobProcedureDomain, Tuple<String, Tuple<String, Integer>> executorTaskDomain) {
		// C = CONCATENATION
		return "C{" + jobProcedureDomain(jobProcedureDomain) + "}:::{" + executorTaskDomain(executorTaskDomain) + "}";
	}

	public static void main(String[] args) {
		Job job = Job.create("TEST").addSubsequentProcedure(WordCountMapper.create());
		Task task = Task.create("hello", job.previousProcedure().jobProcedureDomain()).addFinalExecutorTaskDomainPart(Tuple.create("Executor1", 0));
		System.err.println("Current jobProcedureDomain: " + DomainProvider.INSTANCE.jobProcedureDomain(job.previousProcedure().jobProcedureDomain()));
		System.err.println(
				"Subsequent jobProcedureDomain: " + DomainProvider.INSTANCE.jobProcedureDomain(job.currentProcedure().jobProcedureDomain()));
		Tuple<String, Tuple<String, Integer>> executorTaskDomain = task.executorTaskDomain(Tuple.create("Executor1", 0));
		if (executorTaskDomain != null) {
			System.err.println("executorTaskDomain: " + DomainProvider.INSTANCE.executorTaskDomain(executorTaskDomain));
			System.err.println(
					"concatenation: " + DomainProvider.INSTANCE.concatenation(job.previousProcedure().jobProcedureDomain(), executorTaskDomain));
		}
	}

}
