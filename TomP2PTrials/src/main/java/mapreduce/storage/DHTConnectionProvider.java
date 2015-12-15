package mapreduce.storage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

/**
 * Wrapper that abstracts the dht access to convenience methods
 * 
 * @author Oliver
 *
 */
public class DHTConnectionProvider implements IDHTConnectionProvider {
	private static Logger logger = LoggerFactory.getLogger(DHTConnectionProvider.class);
	private static final String KEY_LOCATION_PREAMBLE = "KEYS";
	/** Provides the actual access to the dht */
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

	@Override
	public DHTConnectionProvider connect() {
		dhtUtils.connect(performBlocking);
		return this;
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
		boolean asList = true;
		dhtUtils.addKVD(keyString, value, domainString, asList, performBlocking, new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
					addTaskKey(task, key);
				} else {
					logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("Exception caught", t);
			}

		});
		// addTaskKey(task, key);
	}

	@Override
	public void addTaskKey(Task task, Object key) {
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, peerAddress());
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		Object value = key;
		boolean asList = false;

		dhtUtils.addKVD(keyString, value, domainString, asList, performBlocking, new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
					// addTaskKey(task, key);
				} else {
					logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("Exception caught", t);
			}

		});
	}

	@Override
	public void addProcedureDataProviderDomain(Job job, Object key, Tuple<PeerAddress, Integer> selectedExecutor) {
		// key here is basically the task id...
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
		String keyString = key.toString();
		String value = DomainProvider.INSTANCE.taskPeerDomain(job.id(), job.procedure(job.currentProcedureIndex()).getClass().getSimpleName(),
				job.currentProcedureIndex(), key.toString(), selectedExecutor.first().peerId().toString(), selectedExecutor.second());
		boolean asList = false;
		dhtUtils.addKVD(keyString, value, domainString, asList, performBlocking, new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
					addProcedureOverallKey(job, key);
				} else {
					logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("Exception caught", t);
			}

		});
	}

	@Override
	public void addProcedureOverallKey(Job job, Object key) {
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		Object value = key;
		boolean asList = false;
		dhtUtils.addKVD(keyString, value, domainString, asList, performBlocking, new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
					// addTaskKey(task, key);
				} else {
					logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("Exception caught", t);
			}

		});
	}

	@Override
	public void getTaskData(Task task, Tuple<PeerAddress, Integer> selectedExecutor, Multimap<Object, Object> taskData) {
		Set<Object> taskKeys = new HashSet<>();
		getTaskKeys(task, selectedExecutor, taskKeys);

		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		boolean asList = true;
		List<Object> valuesCollector = new ArrayList<>();

		for (Object key : taskKeys) {
			String keyString = key.toString();
			dhtUtils.getKD(keyString, valuesCollector, domainString, asList, performBlocking);
			taskData.putAll(key, valuesCollector);
			valuesCollector.clear();
		}
	}

	@Override
	public void getTaskKeys(Task task, Tuple<PeerAddress, Integer> selectedExecutor, Set<Object> keysCollector) {
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		boolean asList = false;
		dhtUtils.getKD(keyString, keysCollector, domainString, asList, performBlocking);
	}

	@Override
	public void getProcedureKeys(Job job, Set<Object> keysCollector) {
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		boolean asList = false;
		dhtUtils.getKD(keyString, keysCollector, domainString, asList, performBlocking);
	}

	@Override
	public void getProcedureTaskPeerDomains(Job job, Object key, Set<Object> domainsCollector) {
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
		String keyString = key.toString();
		boolean asList = false;
		dhtUtils.getKD(keyString, domainsCollector, domainString, asList, performBlocking);
	}

	@Override
	public void removeTaskResultsFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor) {
		Set<Object> keys = new HashSet<>();
		getTaskKeys(task, selectedExecutor, keys);
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		for (final Object key : keys) {
			dhtUtils.removeKD(domainString, key.toString(), performBlocking);
		}
	}

	@Override
	public void removeTaskKeysFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor) {
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		dhtUtils.removeKD(domainString, keyString, performBlocking);
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
