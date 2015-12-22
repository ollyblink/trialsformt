package mapreduce.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import mapreduce.manager.broadcasthandler.broadcastmessages.JobFailedBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.JobUpdateBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.manager.conditions.ListContainsFalseCondition;
import mapreduce.manager.conditions.ListSizeZeroCondition;
import mapreduce.storage.futureListener.GetFutureListener;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.TimeToLive;
import mapreduce.utils.Tuple;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;

/**
 * Wrapper that abstracts the dht access to convenience methods
 * 
 * @author Oliver
 *
 */
public class DHTConnectionProvider implements IDHTConnectionProvider {
	private static Logger logger = LoggerFactory.getLogger(DHTConnectionProvider.class);
	private static final String PROCEDURE_KEYS = "PROCEDURE_KEYS";
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
	public void broadcastJobFailed(Job job) {
		dhtUtils.broadcastJobUpdate(job, JobFailedBCMessage.newInstance().job(job).sender(this.peerAddress()));
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
		dhtUtils.broadcastTaskUpdate(task, TaskUpdateBCMessage.newExecutingTaskInstance().task(task).sender(this.peerAddress()));

	}

	@Override
	public void broadcastFinishedTask(Task task, Number160 resultHash) {
		dhtUtils.broadcastTaskUpdate(task, TaskUpdateBCMessage.newFinishedTaskInstance().resultHash(resultHash).task(task).sender(this.peerAddress()));
	}

	@Override
	public void get(Job job, Task task, List<Object> dataForTask) {

		String keyString = task.id().toString();
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job) + "_" + DomainProvider.INSTANCE.executorTaskDomain(task.id(),
				task.finalDataLocation().first().peerId().toString(), task.finalDataLocation().second());
		boolean asList = true;

		dhtUtils.getKD(keyString, domainString, performBlocking, GetFutureListener.newInstance(keyString, dataForTask, domainString, asList));
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

	@Override
	public void createTasks(Job job, List<Task> procedureTaskCollector) {
		List<Object> allKeysCollector = Collections.synchronizedList(new ArrayList<>());
		final String procedureDomain = DomainProvider.INSTANCE.jobProcedureDomain(job);
		final String keyString = PROCEDURE_KEYS;
		dhtUtils.getKD(keyString, procedureDomain, true, GetFutureListener.newInstance(keyString, allKeysCollector, procedureDomain, false));
		if (TimeToLive.INSTANCE.cancelOnTimeout(allKeysCollector, ListSizeZeroCondition.create())) {
			// GET ALL THE TASK domains for key procedureKey
			while (!allKeysCollector.isEmpty()) {
				Object nextKey = null;
				nextKey = allKeysCollector.remove(0);
				List<Tuple<PeerAddress, Integer>> taskExecutorDomainCollector = Collections.synchronizedList(new ArrayList<>());
				String taskKeyString = nextKey.toString();
				dhtUtils.getKD(taskKeyString, procedureDomain, true, new BaseFutureListener<FutureGet>() {
					@SuppressWarnings("unchecked")
					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							try {
								if (future.dataMap() != null) {
									for (Number640 n : future.dataMap().keySet()) {
										Object valueObject = null;
										valueObject = future.dataMap().get(n).object();

										synchronized (taskExecutorDomainCollector) {
											taskExecutorDomainCollector.add((Tuple<PeerAddress, Integer>) valueObject);
										}
										logger.info("getKVD: Successfully retrieved value for <K, Domain>: <" + keyString + ", " + procedureDomain
												+ ">: " + valueObject);
									}
								} else {
									logger.warn("getKVD: Value for <K, Domain>: <" + keyString + ", " + procedureDomain + "> is null!");
								}
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else {
							logger.error("getKVD: Failed trying to retrieve value for <K, Domain>: <" + keyString + ", " + procedureDomain + ">");
						}
					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						logger.debug("getKVD: Exception caught", t);

					}

				});
				if (TimeToLive.INSTANCE.cancelOnTimeout(taskExecutorDomainCollector, ListSizeZeroCondition.create())) {
					while (!taskExecutorDomainCollector.isEmpty()) {
						Tuple<PeerAddress, Integer> nextTaskExecutorDomain = null;
						synchronized (taskExecutorDomainCollector) {
							nextTaskExecutorDomain = taskExecutorDomainCollector.remove(0);
						}
						List<Object> dataForKey = Collections.synchronizedList(new ArrayList<>());
						dhtUtils.getKD(taskKeyString, procedureDomain, false,
								GetFutureListener.newInstance(keyString, dataForKey, nextTaskExecutorDomain.toString(), true));
						while (TimeToLive.INSTANCE.cancelOnTimeout(dataForKey, ListSizeZeroCondition.create())) {
							if (taskExecutorDomainCollector.size() > 0) {
								Task task = Task.newInstance(keyString, job.id()).finalDataLocation(taskExecutorDomainCollector.get(0));
								if (!procedureTaskCollector.contains(task)) {
									procedureTaskCollector.add(task);
								}
							}
						}
					}
				}
			}
		}
		// Collections.sort(procedureTaskCollector);
	}

	@Override
	public FuturePut add(String key, Object value, String taskExecutorDomain, boolean asList) {
		return dhtUtils.addKVD(key, value, taskExecutorDomain, asList);
	}

}

// @Override
// public void addData(Job job, Task task, String value, List<Boolean> taskDataSubmitted, int taskDataSubmittedIndexToSet) {
//
// String procedureDomainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
// String taskKeyString = task.id();
// String domainString;
// boolean asList;
// dhtUtils.addKVD(keyString, value, domainString, asList, performBlocking, new BaseFutureListener<FuturePut>() {
//
// @Override
// public void operationComplete(FuturePut future) throws Exception {
// if (future.isSuccess()) {
// logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// addTaskKey(task, key);
// } else {
// logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// }
// }
//
// @Override
// public void exceptionCaught(Throwable t) throws Exception {
// logger.debug("Exception caught", t);
// }
//
// });
// }

// @Override
// public void addTaskData(Task task, final Object key, final Object value) {
// String domainString = DomainProvider.INSTANCE.executorTaskDomain(task.finalDataLocation(Tuple.create(peerAddress(), 0))) + "_"
// + DomainProvider.INSTANCE.jobProcedureDomain(task.jobId(), task.procedure().getClass().getSimpleName(), task.procedureIndex());
// System.err.println(domainString);
// String keyString = key.toString();
// boolean asList = true;
// dhtUtils.addKVD(keyString, value, domainString, asList, performBlocking, new BaseFutureListener<FuturePut>() {
//
// @Override
// public void operationComplete(FuturePut future) throws Exception {
// if (future.isSuccess()) {
// logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// addTaskKey(task, key);
// } else {
// logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// }
// }
//
// @Override
// public void exceptionCaught(Throwable t) throws Exception {
// logger.debug("Exception caught", t);
// }
//
// });
// // addTaskKey(task, key);
// }

// @Override
// public void addTaskKey(Task task, Object key) {
// String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, peerAddress());
// String keyString = KEYS + domainString;
// Object value = key;
// boolean asList = false;
//
// dhtUtils.addKVD(keyString, value, domainString, asList, performBlocking, new BaseFutureListener<FuturePut>() {
//
// @Override
// public void operationComplete(FuturePut future) throws Exception {
// if (future.isSuccess()) {
// logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// // addTaskKey(task, key);
// } else {
// logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// }
// }
//
// @Override
// public void exceptionCaught(Throwable t) throws Exception {
// logger.debug("Exception caught", t);
// }
//
// });
// }

// @Override
// public void addProcedureDataProviderDomain(Job job, Object key, Tuple<PeerAddress, Integer> selectedExecutor) {
// // key here is basically the task id...
// String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
// String keyString = key.toString();
// String value = DomainProvider.INSTANCE.taskPeerDomain(job.id(), job.procedure(job.currentProcedureIndex()).getClass().getSimpleName(),
// job.currentProcedureIndex(), key.toString(), selectedExecutor.first().peerId().toString(), selectedExecutor.second());
// boolean asList = false;
// dhtUtils.addKVD(keyString, value, domainString, asList, performBlocking, new BaseFutureListener<FuturePut>() {
//
// @Override
// public void operationComplete(FuturePut future) throws Exception {
// if (future.isSuccess()) {
// logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// addProcedureOverallKey(job, key);
// } else {
// logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// }
// }
//
// @Override
// public void exceptionCaught(Throwable t) throws Exception {
// logger.debug("Exception caught", t);
// }
//
// });
// }

// @Override
// public void addProcedureOverallKey(Job job, Object key) {
// String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
// String keyString = KEYS + domainString;
// Object value = key;
// boolean asList = false;
// dhtUtils.addKVD(keyString, value, domainString, asList, performBlocking, new BaseFutureListener<FuturePut>() {
//
// @Override
// public void operationComplete(FuturePut future) throws Exception {
// if (future.isSuccess()) {
// logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// // addTaskKey(task, key);
// } else {
// logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
// }
// }
//
// @Override
// public void exceptionCaught(Throwable t) throws Exception {
// logger.debug("Exception caught", t);
// }
//
// });
// }
//
// @Override
// public void getTaskKeys(Task task, Tuple<PeerAddress, Integer> selectedExecutor, Set<Object> keysCollector) {
// String domainString = DomainProvider.INSTANCE.executorTaskDomain(task, selectedExecutor.first(), selectedExecutor.second());
// String keyString = KEYS + domainString;
// boolean asList = false;
// dhtUtils.getKD(keyString, keysCollector, domainString, asList, performBlocking);
// }
//
// @Override
// public void getProcedureKeys(Job job, Set<Object> keysCollector) {
// String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
// String keyString = KEYS + domainString;
// boolean asList = false;
// dhtUtils.getKD(keyString, keysCollector, domainString, asList, performBlocking);
// }
//
// @Override
// public void getProcedureTaskPeerDomains(Job job, Object key, Set<Object> domainsCollector) {
// String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
// String keyString = key.toString();
// boolean asList = false;
// dhtUtils.getKD(keyString, domainsCollector, domainString, asList, performBlocking);
// }
//
// @Override
// public void removeTaskResultsFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor) {
// Set<Object> keys = new HashSet<>();
// getTaskKeys(task, selectedExecutor, keys);
// String domainString = DomainProvider.INSTANCE.executorTaskDomain(task, selectedExecutor.first(), selectedExecutor.second());
// for (final Object key : keys) {
// dhtUtils.removeKD(domainString, key.toString(), performBlocking);
// }
// }
//
// @Override
// public void removeTaskKeysFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor) {
// String domainString = DomainProvider.INSTANCE.executorTaskDomain(task, selectedExecutor.first(), selectedExecutor.second());
// String keyString = KEYS + domainString;
// dhtUtils.removeKD(domainString, keyString, performBlocking);
// }