package mapreduce.execution.broadcasthandler.messageconsumer;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.BCStatusType;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import net.tomp2p.peers.PeerAddress;

public interface IMessageConsumer extends Runnable {

	public void handleNewExecutorOnline();
	
	public void handleReceivedJob(Job job);

	public void handleTaskExecutionStatusUpdate(String jobId, String taskId, PeerAddress peerAddress, BCStatusType currentStatus);

	public void handleFinishedAllTasks(String jobId, Collection<Task> tasks, PeerAddress sender);

	public void handleFinishedJob(String jobId, String jobSubmitterId);

	public BlockingQueue<IBCMessage> queue();

	public BlockingQueue<Job> jobs();

	public IMessageConsumer canTake(boolean canTake);

	public boolean canTake();

}
