package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.concurrent.BlockingQueue;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public interface IMessageConsumer extends Runnable {

	public void handleNewExecutorOnline();

	public void handleReceivedJob(Job job);

	public void handleTaskExecutionStatusUpdate(Task task, Tuple<PeerAddress, BCMessageStatus> toUpdate);

	public void handleFinishedTaskComparion(Task task);

	public void updateJob(Job job, BCMessageStatus status, PeerAddress sender);

	public void handleFinishedJob(Job job, String jobSubmitterId);

	public BlockingQueue<IBCMessage> queue();

	public BlockingQueue<Job> jobs();

	public IMessageConsumer canTake(boolean canTake);

	public boolean canTake();

}
