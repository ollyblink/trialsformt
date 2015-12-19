package mapreduce.storage;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Multimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public interface IDHTConnectionProvider {

	// public void addData(Job job, String taskId, String taskKey, String value, List<Boolean> taskDataSubmitted, int index);

	/**
	 * 
	 * @param job
	 *            needed for the job domain to put this data into
	 * @param taskKey
	 *            defines a task's initial key
	 * @param value
	 *            the value to be stored for that task key
	 * @param taskDataSubmitted
	 *            contains boolean values for each data item that is initially set to false and will be flipped to true when all data items were added
	 * @param index
	 *            taskDataSubmitted.set(true, index), when everything completed, such that the MRJobSubmissionManager can continue working
	 */
	public void addData(Job job, Task task, String value, List<Boolean> taskDataSubmitted, int taskDataSubmittedIndexToSet);

	public void createTasks(Job job, List<Task> procedureTasks, long timeToLive);

	public void getTaskData(Job job, Task task, List<Object> dataForTask);

	// DHT access

	// removeProcedureKey, removeProcedureTaskPeerDomain,

	// Broadcasts
	public MRBroadcastHandler broadcastHandler();

	public void broadcastNewJob(Job job);

	public void broadcastExecutingTask(Task task);

	public void broadcastFinishedTask(Task task, Number160 resultHash);

	public void broadcastFinishedAllTasks(Job job);

	public void broadcastFinishedJob(Job job);

	// Maintenance
	public IDHTConnectionProvider connect();

	public void shutdown();

	public PeerAddress peerAddress();

	public IDHTConnectionProvider performBlocking(boolean performBlocking);

	public boolean isBootstrapper();

	public String bootstrapIP();

	public int bootstrapPort();

}
