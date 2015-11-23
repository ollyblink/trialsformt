package mapreduce.execution.broadcasthandler.broadcastmessages;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

public enum JobStatus {
	FINISHED_JOB, FINISHED_ALL_TASKS,FINISHED_TASK,EXECUTING_TASK,TASK_FAILED,DISTRIBUTED_JOB;
	

	 

	public static void main(String[] args) {
		PriorityBlockingQueue<IBCMessage> stati = new PriorityBlockingQueue<IBCMessage>();
		stati.add(FinishedAllTasksBCMessage.newFinishedAllTasksBCMessage()); 
		stati.add(DistributedJobBCMessage.newDistributedTaskBCMessage()); 
		stati.add(FinishedJobBCMessage.newFinishedJobBCMessage()); 
		stati.add(ExecuteOrFinishedTaskMessage.newFinishedTaskBCMessage()); 
		stati.add(ExecuteOrFinishedTaskMessage.newTaskAssignedBCMessage());  
		stati.add(FinishedAllTasksBCMessage.newFinishedAllTasksBCMessage()); 
		stati.add(DistributedJobBCMessage.newDistributedTaskBCMessage()); 
		stati.add(FinishedJobBCMessage.newFinishedJobBCMessage());    
		stati.add(ExecuteOrFinishedTaskMessage.newFinishedTaskBCMessage()); 
		stati.add(ExecuteOrFinishedTaskMessage.newTaskAssignedBCMessage());  
		stati.add(FinishedAllTasksBCMessage.newFinishedAllTasksBCMessage()); 
		stati.add(DistributedJobBCMessage.newDistributedTaskBCMessage()); 
		stati.add(FinishedJobBCMessage.newFinishedJobBCMessage()); 

		while (!stati.isEmpty()) {
			System.out.println(stati.poll());
		}

	}
}
