package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import net.tomp2p.peers.PeerAddress;

public interface IMessageConsumer extends Runnable {

	public void handleNewExecutorOnline();

	public void handleReceivedJob(Job job);

	public void handleTaskExecutionStatusUpdate(Task task, TaskResult toUpdate);

	public void updateJob(Job job, PeerAddress sender);

	public void handleFinishedJob(Job job, String jobSubmitterId);

	public BlockingQueue<IBCMessage> queue();

	public CopyOnWriteArrayList<Job> jobs();

	public IMessageConsumer canTake(boolean canTake);

	public boolean canTake();

}
