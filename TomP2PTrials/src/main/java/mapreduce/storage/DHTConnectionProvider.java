package mapreduce.storage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.manager.broadcasthandler.broadcastmessages.DistributedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.FinishedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.JobUpdateBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class DHTConnectionProvider implements IDHTConnectionProvider {
	private static final String KEY_LOCATION_PREAMBLE = "KEYS_FOR_";
	private static Logger logger = LoggerFactory.getLogger(DHTConnectionProvider.class);
	private DHTUtils dhtUtils;
	/** Determines if dht operations should be performed in parallel or not */
	private boolean performBlocking;

	private DHTConnectionProvider(DHTUtils dhtUtils) {
		this.dhtUtils = dhtUtils;
	}

	public static DHTConnectionProvider newInstance(DHTUtils dhtUtils) {
		return new DHTConnectionProvider(dhtUtils).performBlocking(true);
	}

	// GETTER/SETTER START
	// ======================
	@Override
	public boolean isBootstrapper() {
		return dhtUtils.isBootstrapper();
	}

	@Override
	public String bootstrapIP() {
		return dhtUtils.bootstrapIP();
	}

	@Override
	public int bootstrapPort() {
		return this.dhtUtils.bootstrapPort();
	}

	@Override
	public MRBroadcastHandler broadcastHandler() {
		return this.dhtUtils.broadcastHandler();
	}

	// GETTER/SETTER FINISHED
	// ======================

	/**
	 * Creates a BroadcastHandler and Peer and connects to the DHT. If a bootstrap port and ip were provided (meaning, there are already peers
	 * connected to a DHT), it will be bootstrap to that node.
	 * 
	 * @param performBlocking
	 */
	@Override
	public void connect() {
		dhtUtils.connect(performBlocking);
	}

	@Override
	public void broadcastNewJob(Job job) {
		dhtUtils.broadcastJobUpdate(job, DistributedJobBCMessage.newInstance().job(job).sender(this.peerAddress()));
	}

	@Override
	public void broadcastFinishedAllTasks(Job job) {
		dhtUtils.broadcastJobUpdate(job, JobUpdateBCMessage.newFinishedAllTasksBCMessage().job(job).sender(this.peerAddress()));

	}

	@Override
	public void broadcastFinishedJob(Job job) {
		dhtUtils.broadcastJobUpdate(job, FinishedJobBCMessage.newInstance().jobSubmitterId(job.jobSubmitterID()).job(job).sender(this.peerAddress()));
	}

	@Override
	public void broadcastExecutingTask(Task task) {
		dhtUtils.broadcastTask(task, TaskUpdateBCMessage.newExecutingTaskInstance().task(task).sender(this.peerAddress()));

	}

	@Override
	public void broadcastFinishedTask(Task task, Number160 resultHash) {
		dhtUtils.broadcastTask(task, TaskUpdateBCMessage.newFinishedTaskInstance().resultHash(resultHash).task(task).sender(this.peerAddress()));
	}

	@Override
	public void addTaskData(Task task, final Object key, final Object value) {
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, peerAddress());
		String keyString = key.toString();

		dhtUtils.addKVD(keyString, value, domainString, true, performBlocking);
		addTaskKey(task, key);
	}

	@Override
	public void addTaskKey(Task task, Object key) {
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, peerAddress());
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		Object value = key;

		dhtUtils.addKVD(keyString, value, domainString, false, performBlocking);
	}

	@Override
	public Multimap<Object, Object> getTaskData(Task task, Tuple<PeerAddress, Integer> selectedExecutor) {
		final Multimap<Object, Object> taskKeyValues = ArrayListMultimap.create();
		Set<Object> taskKeys = getTaskKeys(task, selectedExecutor);

		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		boolean asList = true;
		for (Object key : taskKeys) {
			String keyString = key.toString();
			List<Object> values = dhtUtils.getKD(keyString, domainString, asList, performBlocking);
			taskKeyValues.putAll(key, values);
		}
		return taskKeyValues;
	}

	@Override
	public Set<Object> getTaskKeys(Task task, Tuple<PeerAddress, Integer> selectedExecutor) {
		Set<Object> keys = new HashSet<Object>();
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		boolean asList = false;

		List<Object> keysList = dhtUtils.getKD(keyString, domainString, asList, performBlocking);
		keys.addAll(keysList);

		return keys;
	}

	@Override
	public void addProcedureTaskPeerDomain(Task task, Object key, Tuple<PeerAddress, Integer> selectedExecutor) {
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(task.jobId(), task.procedure().getClass().getSimpleName(),
				task.procedureIndex());
		String keyString = key.toString();
		String value = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());

		dhtUtils.addKVD(keyString, value, domainString, false, performBlocking);
		addProcedureKey(task, key);
	}

	@Override
	public void addProcedureKey(Task task, Object key) {
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(task.jobId(), task.procedure().getClass().getSimpleName(),
				task.procedureIndex());
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		Object value = key;

		dhtUtils.addKVD(keyString, value, domainString, false, performBlocking);
	}

	@Override
	public Set<Object> getProcedureKeys(Job job) {
		Set<Object> keys = new HashSet<Object>();
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		boolean asList = false;
		List<Object> keysList = dhtUtils.getKD(keyString, domainString, asList, performBlocking);
		keys.addAll(keysList);

		return keys;
	}

	@Override
	public Set<Object> getProcedureTaskPeerDomains(Job job, Object key) {
		Set<Object> domains = new HashSet<Object>();
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
		String keyString = key.toString();
		boolean asList = false;

		List<Object> keysList = dhtUtils.getKD(domainString, keyString, asList, performBlocking);
		domains.addAll(keysList);

		return domains;
	}

	@Override
	public void removeTaskResultsFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor) {
//		Set<Object> keys = getTaskKeys(task, selectedExecutor, performBlocking);
//		String domain = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
//		Number160 domainKey = Number160.createHash(domain);
//
//		for (final Object key : keys) {
//			logger.info("removeTaskResultsFor: dHashtable.remove(" + key + ").domain(" + domain + ")");
//			connectionPeer.remove(Number160.createHash(key.toString())).domainKey(domainKey).all().start().awaitUninterruptibly()
//					.addListener(new BaseFutureListener<FutureRemove>() {
//
//						@Override
//						public void operationComplete(FutureRemove future) throws Exception {
//							if (future.isSuccess()) {
//								logger.warn("Successfully removed data for key " + key);
//							} else {
//								logger.warn("No success on trying to remove data for key " + key + ".");
//							}
//						}
//
//						@Override
//						public void exceptionCaught(Throwable t) throws Exception {
//							logger.debug("Exception caught", t);
//						}
//					}
//
//			);
//		}

	}

	@Override
	public void removeTaskKeysFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor) {
		//
		// String domain = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		// String keyLocation = KEY_LOCATION_PREAMBLE + domain;
		// Number160 domainKey = Number160.createHash(domain);
		// Number160 keyLocationHash = Number160.createHash(keyLocation);
		//
		// logger.info("removeTaskKeysFor: dHashtable.remove(" + keyLocation + ").domain(" + domain + ")");
		// connectionPeer.remove(keyLocationHash).domainKey(domainKey).all().start().awaitUninterruptibly()
		// .addListener(new BaseFutureListener<FutureRemove>() {
		//
		// @Override
		// public void operationComplete(FutureRemove removeFuture) throws Exception {
		// if (removeFuture.isSuccess()) {
		// logger.warn("Successfully removed keys");
		// } else {
		// logger.warn("Something wrong trying to remove keys.");
		// }
		// }
		//
		// @Override
		// public void exceptionCaught(Throwable t) throws Exception {
		// logger.debug("Exception caught", t);
		//
		// }
		// }
		//
		// );
	}

	@Override
	public void shutdown() {
		dhtUtils.shutdown();
	}

	@Override
	public PeerAddress peerAddress() {
		return dhtUtils.peerAddress();
	}

	@Override
	public DHTConnectionProvider performBlocking(boolean performBlocking) {
		this.performBlocking = performBlocking;
		return this;
	}

}
