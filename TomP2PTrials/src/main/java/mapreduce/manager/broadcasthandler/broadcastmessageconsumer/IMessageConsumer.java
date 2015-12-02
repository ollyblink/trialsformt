package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCStatusType;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public interface IMessageConsumer extends Runnable {

	public void handleNewExecutorOnline();

	public void handleReceivedJob(Job job);

	public void handleTaskExecutionStatusUpdate(String jobId, String taskId, Tuple<PeerAddress, BCStatusType> toUpdate);

	public void handleFinishedAllTasks(String jobId, Collection<Task> tasks, PeerAddress sender);

	public void handleFinishedTaskComparion(String jobId, String taskId, Tuple<PeerAddress, Integer> finalDataLocation);

	public void handleFinishedAllTaskComparisons(String jobId, Collection<Task> tasks, PeerAddress sender);

	public void handleFinishedJob(String jobId, String jobSubmitterId);

	public BlockingQueue<IBCMessage> queue();

	public BlockingQueue<Job> jobs();

	public IMessageConsumer canTake(boolean canTake);

	public boolean canTake();

}
