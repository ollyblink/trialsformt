package mapreduce.storage;

import java.util.List;

import com.google.common.collect.Multimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public interface IDHTConnectionProvider {

	public boolean isBootstrapper();

	public IDHTConnectionProvider isBootstrapper(boolean isBootstrapper);

	public String bootstrapIP();

	public int bootstrapPort();

	public IDHTConnectionProvider bootstrapIP(String bootstrapIP);

	public IDHTConnectionProvider bootstrapPort(int bootstrapPort);

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
	public Multimap<Object, Object> getTaskData(final Task task, final Tuple<PeerAddress, Integer> selectedExecutor);

	public List<Object> getTaskKeys(final Task task, final Tuple<PeerAddress, Integer> selectedExecutor);

	public void removeTaskResultsFor(final Task task, final Tuple<PeerAddress, Integer> selectedExecutor);

	public void removeTaskKeysFor(final Task task, final Tuple<PeerAddress, Integer> selectedExecutor);

	public void addProcedureKey(final Task task, final Object key, final boolean awaitUninterruptibly);

	public void addProcedureTaskPeerDomain(final Task task, final Object key, final Tuple<PeerAddress, Integer> selectedExecutor,
			final boolean awaitUninterruptibly);

	// getProcedureKey, getProcedureTaskPeerDomain, removeProcedureKey, removeProcedureTaskPeerDomain,

	// Broadcasts
	public MRBroadcastHandler broadcastHandler();

	public void broadcastNewJob(Job job);

	public void broadcastExecutingTask(Task task);

	public void broadcastFinishedTask(Task task, Number160 resultHash);

	public void broadcastFinishedAllTasks(Job job);

	public void broadcastFinishedJob(Job job);

	// Maintenance
	public IDHTConnectionProvider connect();

	public boolean alreadyExecuted(Task task);

	public void shutdown();

	public PeerAddress peerAddress();

	public String storageFilePath();

	public IDHTConnectionProvider storageFilePath(String storageFilePath);

}
