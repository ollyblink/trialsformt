package mapreduce.storage;

import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasting.MRBroadcastHandler;
import mapreduce.manager.broadcasting.broadcastmessages.CompletedBCMessage;
import mapreduce.manager.broadcasting.broadcastmessages.IBCMessage;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.storage.Data;

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
	 */
	public FuturePut add(String key, Object value, String domain, boolean asList);

	public FuturePut addAll(String key, Collection<Data> values, String domain);

	public FuturePut put(String key, Object value, String domain);

	// public void createTasks(Job job, List<FutureGet> procedureTaskFutureGetCollector, List<Task> procedureTaskCollector);

	public FutureGet getAll(String keyString, String domainString);

	public FutureGet get(String job, String receivedJobId);

	// DHT access

	// removeProcedureKey, removeProcedureTaskPeerDomain,

	// Broadcasts

	// public JobDistributedBCMessage broadcastNewJob(Job job);
	//
	// // public JobFailedBCMessage broadcastFailedJob(Job job);
	//
	// public TaskCompletedBCMessage broadcastExecutingTask(Task task, Tuple<String, Integer> taskExecutor);
	//
	// // public TaskUpdateBCMessage broadcastFailedTask(Task taskToDistribute);
	//
	// public TaskCompletedBCMessage broadcastFinishedTask(Task task, Tuple<String, Integer> taskExecutor, Number160 resultHash);
	//
	// public ProcedureCompletedBCMessage broadcastFinishedAllTasksOfProcedure(Job job);
	//
	// public JobFinishedBCMessage broadcastFinishedJob(Job job);

	// Maintenance

	/**
	 * Creates a BroadcastHandler and Peer and connects to the DHT. If a bootstrap port and ip were provided (meaning, there are already peers
	 * connected to a DHT), it will be bootstrap to that node.
	 * 
	 * @param performBlocking
	 * @return
	 */
	public void connect();

	public void shutdown();

	// public PeerAddress peerAddress();

	// public IDHTConnectionProvider performBlocking(boolean performBlocking);

	public boolean isBootstrapper();

	public String bootstrapIP();

	public int bootstrapPort();

	/**
	 * ID of the JobExecutionManager or SubmissionManager
	 * 
	 * @return
	 */
	public String owner();

	public IDHTConnectionProvider owner(String owner);

	public IDHTConnectionProvider port(int port);

	public int port();

	public String storageFilePath();

	public IDHTConnectionProvider storageFilePath(String storageFilePath);

	public IDHTConnectionProvider bootstrapPort(int bootstrapPort);

	public IDHTConnectionProvider isBootstrapper(boolean isBootstrapper);

	public MRBroadcastHandler broadcastHandler();

	public void broadcastCompletion(CompletedBCMessage completedMessage);

	public IDHTConnectionProvider jobQueues(TreeMap<Job, PriorityBlockingQueue<IBCMessage>> jobs);

}
