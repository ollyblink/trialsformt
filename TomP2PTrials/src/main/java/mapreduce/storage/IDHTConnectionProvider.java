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

	// DHT access 
	public void addTaskData(Task task, Object key, Object value); 
	public void addTaskKey(Task task, Object key);

	public void addProcedureOverallKey(Job job, Object key);

	public void addProcedureDataProviderDomain(Job job, Object key, Tuple<PeerAddress, Integer> selectedExecutor);

	public void getTaskData(Task task, Tuple<PeerAddress, Integer> selectedExecutor, Multimap<Object, Object> taskData);

	public void getTaskKeys(Task task, Tuple<PeerAddress, Integer> selectedExecutor, Set<Object> keysCollector);

	public void getProcedureKeys(Job job, Set<Object> keysCollector);

	public void getProcedureTaskPeerDomains(Job job, Object key, Set<Object> domainsCollector);

	public void removeTaskResultsFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor);

	public void removeTaskKeysFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor);

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
