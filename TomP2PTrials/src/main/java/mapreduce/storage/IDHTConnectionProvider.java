package mapreduce.storage;

import java.util.List;

import com.google.common.collect.Multimap;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import net.tomp2p.peers.PeerAddress;

public interface IDHTConnectionProvider {

	// DHT access
	/**
	 * to store the data for each key produced by this peer
	 * 
	 * @param task
	 * @param key
	 * @param value
	 */
	public void addTaskData(Task task, final Object key, final Object value, boolean awaitUninterruptibly);

	/**
	 * to store the keys produced for this task by this peer
	 * 
	 * @param task
	 * @param key
	 */
	public void addTaskKey(final Task task, final Object key, boolean awaitUninterruptibly);

	/**
	 * 
	 * @param task
	 * @param jobStatusIndex
	 *            represents the index in task.executingPeers for which the data should be collected
	 * @return
	 */
	public Multimap<Object, Object> getTaskData(Task task, LocationBean locationBean);

	public List<Object> getTaskKeys(Task task, LocationBean locationBean);

	public void removeTaskResultsFor(Task task, LocationBean locationBean);

	void removeTaskKeysFor(Task task, LocationBean locationBean);

	// Broadcasts
	public MRBroadcastHandler broadcastHandler();

	public void broadcastNewJob(Job job);

	public void broadcastExecutingTask(Task task);

	public void broadcastFinishedTask(Task task);

	public void broadcastFinishedAllTasks(Job job);

	public void broadcastExecutingCompareTaskResults(Task task);

	public void broadcastFinishedCompareTaskResults(Task task);

	public void broadcastFinishedAllTaskComparisons(Job job);

	public void broadcastFinishedJob(Job job);

	// Maintenance
	public IDHTConnectionProvider connect();

	public boolean alreadyExecuted(Task task);

	public void shutdown();

	public IDHTConnectionProvider useDiskStorage(boolean useDiskStorage);

	public boolean useDiskStorage();

	public String peerAddressString();

	public PeerAddress peerAddress();

}
