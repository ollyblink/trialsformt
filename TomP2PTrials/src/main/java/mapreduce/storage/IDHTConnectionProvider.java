package mapreduce.storage;

import java.util.Set;

import com.google.common.collect.Multimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public interface IDHTConnectionProvider {

	public boolean isBootstrapper();

	public String bootstrapIP();

	public int bootstrapPort();

	// DHT access
	/**
	 * to store the data for each key produced by this peer
	 * 
	 * @param task
	 * @param key
	 * @param value
	 */
	public void addTaskData(Task task, Object key, Object value);

	/**
	 * to store the keys produced for this task by this peer
	 * 
	 * @param task
	 * @param key
	 */
	public void addTaskKey(Task task, Object key);

	/**
	 * 
	 * @param task
	 * @param jobStatusIndex
	 *            represents the index in task.executingPeers for which the data should be collected
	 * @return
	 */
	public Multimap<Object, Object> getTaskData(Task task, Tuple<PeerAddress, Integer> selectedExecutor);

	public Set<Object> getTaskKeys(Task task, Tuple<PeerAddress, Integer> selectedExecutor);

	public void removeTaskResultsFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor);

	public void removeTaskKeysFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor);

	public void addProcedureKey(Task task, Object key);

	public void addProcedureTaskPeerDomain(Task task, Object key, Tuple<PeerAddress, Integer> selectedExecutor);

	public Set<Object> getProcedureKeys(Job job);

	public Set<Object> getProcedureTaskPeerDomains(Job job, Object key);

	// removeProcedureKey, removeProcedureTaskPeerDomain,

	// Broadcasts
	public MRBroadcastHandler broadcastHandler();

	public void broadcastNewJob(Job job);

	public void broadcastExecutingTask(Task task);

	public void broadcastFinishedTask(Task task, Number160 resultHash);

	public void broadcastFinishedAllTasks(Job job);

	public void broadcastFinishedJob(Job job);

	// Maintenance
	public void connect();

	public void shutdown();

	public PeerAddress peerAddress();
	
	public IDHTConnectionProvider performBlocking(boolean performBlocking);

}
