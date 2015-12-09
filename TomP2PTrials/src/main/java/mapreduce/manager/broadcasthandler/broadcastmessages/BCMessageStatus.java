package mapreduce.manager.broadcasthandler.broadcastmessages;

public enum BCMessageStatus {
	NEW_EXECUTOR_ONLINE, FINISHED_JOB, FINISHED_ALL_TASKS, EXECUTING_TASK, FINISHED_TASK, TASK_FAILED, DISTRIBUTED_JOB;
}
