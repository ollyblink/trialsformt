package mapreduce.manager.broadcasthandler.broadcastmessages;

public enum BCMessageStatus {
	FAILED_JOB, NEW_EXECUTOR_ONLINE, FINISHED_JOB, EXECUTING_TASK, FINISHED_TASK, FAILED_TASK, FINISHED_PROCEDURE, DISTRIBUTED_JOB;
}
