package mapreduce.storage;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.peers.Number160;

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

	public FuturePut put(String key, Object value, String domain);

	public void createTasks(Job job, List<FutureGet> procedureTaskFutureGetCollector, List<Task> procedureTaskCollector);

	public FutureGet getAll(String keyString, String domainString);

	// DHT access

	// removeProcedureKey, removeProcedureTaskPeerDomain,

	// Broadcasts

	public void broadcastNewJob(Job job);

	public void broadcastFailedJob(Job job);

	public void broadcastExecutingTask(Task task);

	public void broadcastFailedTask(Task taskToDistribute);

	public void broadcastFinishedTask(Task task, Number160 resultHash);

	public void broadcastFinishedAllTasksOfProcedure(Job job);

	public void broadcastFinishedJob(Job job);

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

	public IDHTConnectionProvider addMessageQueueToBroadcastHandlers(BlockingQueue<IBCMessage> bcMessages);
}
