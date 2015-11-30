package mapreduce.execution.broadcasthandler.broadcastmessages;

import java.util.concurrent.PriorityBlockingQueue;

public enum JobStatus {
	FINISHED_JOB, FINISHED_ALL_TASKS, EXECUTING_TASK, FINISHED_TASK, TASK_FAILED, DISTRIBUTED_JOB;

	public static void main(String[] args) {
		PriorityBlockingQueue<IBCMessage> stati = new PriorityBlockingQueue<IBCMessage>();
		stati.add(DistributedJobBCMessage.newInstance());
		stati.add(ExecuteOrFinishedTaskMessage.newFinishedTaskInstance());
		stati.add(ExecuteOrFinishedTaskMessage.newExecutingTaskInstance());
		stati.add(FinishedAllTasksBCMessage.newInstance());
		stati.add(DistributedJobBCMessage.newInstance());
		stati.add(DistributedJobBCMessage.newInstance());
		stati.add(ExecuteOrFinishedTaskMessage.newFinishedTaskInstance());
		stati.add(ExecuteOrFinishedTaskMessage.newExecutingTaskInstance());
		stati.add(FinishedAllTasksBCMessage.newInstance());
		stati.add(FinishedAllTasksBCMessage.newInstance());
		stati.add(FinishedJobBCMessage.newInstance());
		stati.add(FinishedJobBCMessage.newInstance());
		stati.add(FinishedJobBCMessage.newInstance());

		while (!stati.isEmpty()) {
			System.out.println(stati.poll());
		}

	}
}
