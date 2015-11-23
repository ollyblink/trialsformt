package mapreduce.execution.jobtask;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

public enum JobStatus {
	FINISHED_JOB, FINISHED_ALL_TASKS,FINISHED_TASK,EXECUTING_TASK,TASK_FAILED,DISTRIBUTED_JOB;
	

	 

	public static void main(String[] args) {
		PriorityBlockingQueue<JobStatus> stati = new PriorityBlockingQueue<JobStatus>();
		stati.add(JobStatus.EXECUTING_TASK);
		stati.add(JobStatus.FINISHED_TASK);
		stati.add(JobStatus.FINISHED_ALL_TASKS);
		stati.add(JobStatus.EXECUTING_TASK);
		stati.add(JobStatus.DISTRIBUTED_JOB);
		stati.add(JobStatus.FINISHED_JOB);
		stati.add(JobStatus.TASK_FAILED);
		stati.add(JobStatus.DISTRIBUTED_JOB);
		stati.add(JobStatus.EXECUTING_TASK);
		stati.add(JobStatus.FINISHED_ALL_TASKS);
		stati.add(JobStatus.EXECUTING_TASK);
		stati.add(JobStatus.FINISHED_JOB);
		stati.add(JobStatus.TASK_FAILED);

		while (!stati.isEmpty()) {
			System.out.println(stati.poll());
		}

	}
}
