package mapreduce.manager.broadcasthandler.broadcastmessages;

import java.util.concurrent.PriorityBlockingQueue;

public enum BCStatusType {
	NEW_EXECUTOR_ONLINE, FINISHED_JOB, FINISHED_ALL_TASKS, EXECUTING_TASK_COMPARISON, FINISHED_TASK_COMPARISON, EXECUTING_TASK, FINISHED_TASK, TASK_FAILED, DISTRIBUTED_JOB;

	public static void main(String[] args) {
		PriorityBlockingQueue<IBCMessage> stati = new PriorityBlockingQueue<IBCMessage>();
		stati.add(DistributedJobBCMessage.newInstance());
		stati.add(TaskUpdateBCMessage.newFinishedTaskInstance());
		stati.add(TaskUpdateBCMessage.newExecutingTaskInstance());
		stati.add(FinishedAllTasksBCMessage.newInstance());
		stati.add(DistributedJobBCMessage.newInstance());
		stati.add(DistributedJobBCMessage.newInstance());
		stati.add(TaskUpdateBCMessage.newFinishedTaskInstance());
		stati.add(TaskUpdateBCMessage.newExecutingTaskInstance());
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
