package mapreduce.storage;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.manager.broadcasthandler.broadcastmessages.DistributedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.FinishedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.JobFailedBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.JobUpdateBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Tuple;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

/**
 * Wrapper that abstracts the dht access to convenience methods
 * 
 * @author Oliver
 *
 */
public class DHTConnectionProvider implements IDHTConnectionProvider {
	private static Logger logger = LoggerFactory.getLogger(DHTConnectionProvider.class);
	/** Provides the actual access to the dht */
	private DHTUtils dhtUtils;
	/** Determines if dht operations should be performed in parallel or not */
	private boolean performBlocking;
	private PeerDHT peerDHT;
	private String owner;

	private DHTConnectionProvider(DHTUtils dhtUtils) {
		this.dhtUtils = dhtUtils;
		this.peerDHT = dhtUtils.peerDHT();
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
		broadcastJobUpdate(job, DistributedJobBCMessage.newInstance().job(job).sender(this.owner()));
	}

	@Override
	public void broadcastFailedJob(Job job) {
		broadcastJobUpdate(job, JobFailedBCMessage.newInstance().job(job).sender(this.owner()));
	}

	@Override
	public void broadcastFinishedAllTasksOfProcedure(Job job) {
		broadcastJobUpdate(job, JobUpdateBCMessage.newFinishedAllTasksBCMessage().job(job).sender(this.owner()));

	}

	@Override
	public void broadcastFinishedJob(Job job) {
		broadcastJobUpdate(job, FinishedJobBCMessage.newInstance().job(job).sender(this.owner()));
	}

	@Override
	public void broadcastExecutingTask(Task task) {
		broadcastTaskUpdate(task, TaskUpdateBCMessage.newExecutingTaskInstance().task(task).sender(this.owner()));

	}

	@Override
	public void broadcastFinishedTask(Task task, Number160 resultHash) {
		broadcastTaskUpdate(task, TaskUpdateBCMessage.newFinishedTaskInstance().resultHash(resultHash).task(task).sender(this.owner()));
	}

	public void broadcastTaskUpdate(Task task, IBCMessage message) {
		try {
			int currentStatusIndex = task.executingPeers().get(owner).size() - 1;
			Tuple<String, Integer> taskExecutor = Tuple.create(owner, currentStatusIndex);
			Number160 taskHash = Number160.createHash(DomainProvider.INSTANCE.executorTaskDomain(task, taskExecutor));
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			dataMap.put(new Number640(taskHash, taskHash, taskHash, taskHash), new Data(message));
			peerDHT.peer().broadcast(taskHash).dataMap(dataMap).start();
		} catch (IOException e) {
			logger.warn("Exception thrown in DHTConnectionProvider::broadcastTaskSchedule", e);
		}
	}

	public void broadcastJobUpdate(Job job, IBCMessage message) {
		try {
			Number160 jobHash = Number160.createHash(DomainProvider.INSTANCE.jobProcedureDomain(job));
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			dataMap.put(new Number640(jobHash, jobHash, jobHash, jobHash), new Data(message));
			peerDHT.peer().broadcast(jobHash).dataMap(dataMap).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void shutdown() {
		dhtUtils.shutdown();
	}

	@Override
	public String owner() {
		return this.owner;
	}

	@Override
	public DHTConnectionProvider owner(String owner) {
		this.owner = owner;
		return this;
	}

	@Override
	public DHTConnectionProvider performBlocking(boolean performBlocking) {
		this.performBlocking = performBlocking;
		return this;
	}

	@Override
	public FutureGet getAll(String keyString, String domainString) {
		return dhtUtils.peerDHT().get(Number160.createHash(keyString)).domainKey(Number160.createHash(domainString)).all().start();
	}

	@Override
	public void createTasks(Job job, List<FutureGet> procedureTaskFutureGetCollector, List<Task> procedureTaskCollector) {

		final String procedureDomain = DomainProvider.INSTANCE.jobProcedureDomain(job);
		final Number160 procedureDomainHash = Number160.createHash(procedureDomain);

		dhtUtils.peerDHT().get(Number160.createHash(DomainProvider.PROCEDURE_KEYS)).domainKey(procedureDomainHash).all().start()
				.addListener(new BaseFutureListener<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							try {
								if (future.dataMap() != null) {
									for (Number640 n : future.dataMap().keySet()) {
										Object key = future.dataMap().get(n).object();
										// 1 key from procedure domain == 1 task
										procedureTaskFutureGetCollector.add(dhtUtils.peerDHT().get(Number160.createHash(key.toString()))
												.domainKey(procedureDomainHash).all().start().addListener(new BaseFutureListener<FutureGet>() {

											@Override
											public void operationComplete(FutureGet future) throws Exception {
												if (future.isSuccess()) {
													try {
														if (future.dataMap() != null) {
															for (Number640 n : future.dataMap().keySet()) {
																String taskExecutorDomain = future.dataMap().get(n).object().toString();

																Task task = Task.newInstance(key, job.id())
																		.finalDataLocationDomains(taskExecutorDomain);
																procedureTaskCollector.add(task);
																logger.info("getKVD: Successfully retrieved value for <K, Domain>: <" + key + ", "
																		+ procedureDomain + ">: " + taskExecutorDomain);
															}

														} else {
															logger.warn(
																	"getKVD: Value for <K, Domain>: <" + key + ", " + procedureDomain + "> is null!");
														}
													} catch (ClassNotFoundException | IOException e) {
														e.printStackTrace();
													}
												} else {
													logger.error("getKVD: Failed trying to retrieve value for <K, Domain>: <" + key + ", "
															+ procedureDomain + ">");
												}
											}

											@Override
											public void exceptionCaught(Throwable t) throws Exception {
												logger.debug("getKVD: Exception caught", t);

											}

										}));
									}
								} else {
									logger.warn("getKVD: Value for <K, Domain>: <" + DomainProvider.PROCEDURE_KEYS + ", " + procedureDomain
											+ "> is null!");
								}
							} catch (ClassNotFoundException e)

					{
								e.printStackTrace();
							} catch (

					IOException e)

					{
								e.printStackTrace();
							}

						} else {
							logger.error("getKVD: Failed trying to retrieve value for <K, Domain>: <" + DomainProvider.PROCEDURE_KEYS + ", "
									+ procedureDomain + ">");
						}
					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						logger.debug("get: Exception caught", t);
					}

				});

		// Collections.sort(procedureTaskCollector);
	}

	@Override
	public FuturePut add(String keyString, Object value, String domainString, boolean asList) {
		try {
			logger.info("add: Trying to perform: dHashtable.add(" + keyString + ", " + value + ").domain(" + domainString + ")");
			Data valueData = new Data(value);
			if (asList) {
				valueData = new Data(new Value(value));
			}

			return this.dhtUtils.peerDHT().add(Number160.createHash(keyString)).data(valueData).domainKey(Number160.createHash(domainString)).start();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public FuturePut put(String keyString, Object value, String domainString) {

		try {
			logger.info("put: Trying to perform: dHashtable.add(" + keyString + ", " + value + ").domain(" + domainString + ")");

			return this.dhtUtils.peerDHT().put(Number160.createHash(keyString)).data(new Data(value)).domainKey(Number160.createHash(domainString))
					.start();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void broadcastFailedTask(Task taskToDistribute) {
		// TODO Auto-generated method stub

	}

}
