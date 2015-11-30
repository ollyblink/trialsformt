package mapreduce.storage;

import java.util.List;

import com.google.common.collect.Multimap;

import mapreduce.execution.broadcasthandler.MRBroadcastHandler;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import net.tomp2p.peers.PeerAddress;

public interface IDHTConnectionProvider {

	public IDHTConnectionProvider connect();

	/**
	 * to store the data for each key produced by this peer
	 * 
	 * @param task
	 * @param key
	 * @param value
	 */
	public void addDataForTask(Task task, final Object key, final Object value);

	/**
	 * to store the keys produced for this task by this peer
	 * 
	 * @param task
	 * @param key
	 */
	public void addTaskKey(final Task task, final Object key);

	/**
	 * 
	 * @param task
	 * @param jobStatusIndex
	 *            represents the index in task.executingPeers for which the data should be collected
	 * @return
	 */
	public Multimap<Object, Object> getDataForTask(Task task);

	public List<Object> getTaskKeys(Task task);

	public void broadcastNewJob(Job job);

	public void broadcastExecutingTask(Task task);

	public void broadcastFinishedTask(Task task);

	public boolean alreadyExecuted(Task task);

	public void shutdown();

	public void broadcastFinishedAllTasks(Job job);

	public void broadcastFinishedJob(Job job);

	public MRBroadcastHandler broadcastHandler();

	public IDHTConnectionProvider useDiskStorage(boolean useDiskStorage);

	public boolean useDiskStorage();

	public String peerAddressString();

	public PeerAddress peerAddress();
}
