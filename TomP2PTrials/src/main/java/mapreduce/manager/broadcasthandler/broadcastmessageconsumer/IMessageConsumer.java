package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;

public interface IMessageConsumer extends Runnable {

	public void handleNewExecutorOnline();

	/**
	 * Can either be because a new Job was added, or the next procedure is being executed. If a job with a higher priority is added, the currently
	 * executed procedure of a currently processed job is aborted after the currently executed task finished and the new Job with higher priority is
	 * being executed.
	 * 
	 * @param job
	 */
	public void handleReceivedJob(Job job);

	public void handleFailedJob(Job job);

	public void handleTaskExecutionStatusUpdate(Task task, TaskResult toUpdate);

	public void handleFinishedAllTasks(Job job, String sender);

	public void handleFinishedJob(Job job);

	public BlockingQueue<IBCMessage> queue();

	public List<Job> jobs();

	public IMessageConsumer canTake(boolean canTake);

	public boolean canTake();

}
