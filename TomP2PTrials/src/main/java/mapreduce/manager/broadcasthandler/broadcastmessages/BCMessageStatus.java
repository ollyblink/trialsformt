package mapreduce.manager.broadcasthandler.broadcastmessages;

public enum BCMessageStatus {
	NEW_EXECUTOR_ONLINE, FINISHED_JOB, EXECUTING_TASK, FINISHED_TASK, TASK_FAILED, FINISHED_ALL_TASKS, DISTRIBUTED_JOB;
}
