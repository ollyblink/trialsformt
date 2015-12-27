package mapreduce.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.manager.broadcasthandler.broadcastmessages.DistributedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.FinishedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.FinishedProcedureBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.JobFailedBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Tuple;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageDisk;

/**
 * Wrapper that abstracts the dht access to convenience methods
 * 
 * @author Oliver
 *
 */
public class DHTConnectionProvider implements IDHTConnectionProvider {
	private static final int DEFAULT_NUMBER_OF_PEERS = 10;
	private static Logger logger = LoggerFactory.getLogger(DHTConnectionProvider.class);
	private List<PeerDHT> peerDHTs;
	private List<MRBroadcastHandler> broadcastHandlers;
	private String owner;
	private String bootstrapIP;
	private int bootstrapPort;
	private boolean isBootstrapper;
	private int port;
	// private MRBroadcastHandler broadcastHandler;
	private String id;
	private String storageFilePath;
	private int numberOfPeers = DEFAULT_NUMBER_OF_PEERS;
	private int currentExecutingPeerCounter = 0;

	private DHTConnectionProvider(String bootstrapIP, int bootstrapPort) {
		this.bootstrapIP = bootstrapIP;
		this.bootstrapPort = bootstrapPort;
		this.peerDHTs = SyncedCollectionProvider.syncedArrayList();
		this.broadcastHandlers = SyncedCollectionProvider.syncedArrayList();

	}

	public static DHTConnectionProvider newInstance(String bootstrapIP, int bootstrapPort) {
		return new DHTConnectionProvider(bootstrapIP, bootstrapPort);
	}

	// GETTER/SETTER START
	// ======================
	public int numberOfPeers() {
		return numberOfPeers;
	}

	public DHTConnectionProvider numberOfPeers(int numberOfPeers) {
		this.numberOfPeers = numberOfPeers;
		return this;
	}

	public DHTConnectionProvider externalPeers(List<PeerDHT> peerDHTs, List<MRBroadcastHandler> broadcastHandlers) {
		this.peerDHTs = peerDHTs;
		this.broadcastHandlers = broadcastHandlers;
		return this;
	}

	@Override
	public boolean isBootstrapper() {
		return this.isBootstrapper;
	}

	@Override
	public DHTConnectionProvider addMessageQueueToBroadcastHandlers(BlockingQueue<IBCMessage> bcMessages) {
		for (MRBroadcastHandler bCH : broadcastHandlers) {
			bCH.queue(bcMessages);
		}
		return this;
	}

	@Override
	public DHTConnectionProvider isBootstrapper(boolean isBootstrapper) {
		this.isBootstrapper = isBootstrapper;
		return this;
	}

	@Override
	public String bootstrapIP() {
		return this.bootstrapIP;
	}

	@Override
	public DHTConnectionProvider bootstrapPort(int bootstrapPort) {
		if (this.bootstrapPort == 0 && bootstrapPort > 0) {// make sure it cannot be externally changed...
			this.bootstrapPort = bootstrapPort;
		}
		return this;
	}

	@Override
	public int bootstrapPort() {
		return this.bootstrapPort;
	}

	@Override
	public DHTConnectionProvider port(int port) {
		if (this.port == 0 && port > 0) {// make sure it cannot be externally changed...
			this.port = port;
		}
		return this;
	}

	@Override
	public int port() {
		return this.port;
	}

	@Override
	public String storageFilePath() {
		return storageFilePath;
	}

	@Override
	public DHTConnectionProvider storageFilePath(String storageFilePath) {
		this.storageFilePath = storageFilePath;
		return this;
	}
	// GETTER/SETTER FINISHED
	// ======================

	@Override
	public void connect() {
		int bootstrapper = 0; // I use this to only make one peer be the master that creates everything... all others simply connect to the
								// bootstrapper
		for (int i = 0; i < this.numberOfPeers; ++i) {

			try {
				if (this.isBootstrapper && bootstrapper == 0) {

					this.port = this.bootstrapPort;
				}

				MRBroadcastHandler broadcastHandler = new MRBroadcastHandler();
				broadcastHandlers.add(broadcastHandler);
				Peer peer = new PeerBuilder(Number160.createHash(this.id)).ports(this.port).broadcastHandler(broadcastHandler).start();

				if (!this.isBootstrapper && bootstrapper == 0) {
					this.doBootstrapping(peer);
					++bootstrapper;
				}
				PeerBuilderDHT peerDHTBuilder = new PeerBuilderDHT(peer);

				if (this.storageFilePath != null) {
					peerDHTBuilder.storage(new StorageDisk(peer.peerID(), new File(this.storageFilePath), null));
				}
				this.peerDHTs.add(peerDHTBuilder.start());
			} catch (IOException e) {
				logger.debug("Exception on bootstrapping", e);
			}
		}
	}

	public void doBootstrapping(Peer peer) throws UnknownHostException {

		peer.bootstrap().inetAddress(InetAddress.getByName(bootstrapIP())).ports(bootstrapPort()).start()
				.addListener(new BaseFutureListener<FutureBootstrap>() {

					@Override
					public void operationComplete(FutureBootstrap future) throws Exception {
						if (future.isSuccess()) {
							logger.warn("successfully bootstrapped to " + bootstrapIP + "/" + bootstrapPort);
						} else {
							logger.warn("No success on bootstrapping: fail reason: " + future.failedReason());
						}
					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						logger.warn("Exception on bootstrapping", t);
					}
				});
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
		broadcastJobUpdate(job, FinishedProcedureBCMessage.create().job(job).sender(this.owner()));

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

	@Override
	public void broadcastFailedTask(Task task) {
		broadcastTaskUpdate(task, TaskUpdateBCMessage.newFailedTaskInstance().task(task).sender(this.owner()));

	}

	public void broadcastTaskUpdate(Task task, IBCMessage message) {
		try {
			int currentStatusIndex = task.executingPeers().get(owner).size() - 1;
			Tuple<String, Integer> taskExecutor = Tuple.create(owner, currentStatusIndex);
			Number160 taskHash = Number160.createHash(DomainProvider.INSTANCE.executorTaskDomain(task, taskExecutor));
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			dataMap.put(new Number640(taskHash, taskHash, taskHash, taskHash), new Data(message));
			currentExecutingPeer().peer().broadcast(taskHash).dataMap(dataMap).start();
		} catch (IOException e) {
			logger.warn("Exception thrown in DHTConnectionProvider::broadcastTaskSchedule", e);
		}
	}

	public void broadcastJobUpdate(Job job, IBCMessage message) {
		try {
			Number160 jobHash = Number160.createHash(DomainProvider.INSTANCE.jobProcedureDomain(job));
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			dataMap.put(new Number640(jobHash, jobHash, jobHash, jobHash), new Data(message));
			currentExecutingPeer().peer().broadcast(jobHash).dataMap(dataMap).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void shutdown() {
		for (PeerDHT peerDHT : peerDHTs) {
			BaseFuture shutdown = peerDHT.shutdown();
			shutdown.addListener(new BaseFutureListener<BaseFuture>() {

				@Override
				public void operationComplete(BaseFuture future) throws Exception {
					if (future.isSuccess()) {
						logger.trace("Successfully shut down peer " + peerDHT.peerID() + ".");
					} else {
						logger.trace("Could not shut down peer " + peerDHT.peerID() + ".");
					}
				}

				@Override
				public void exceptionCaught(Throwable t) throws Exception {
					logger.warn("Exception thrown in DHTConnectionProvider::shutdown()", t);
				}
			});
		}
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
	public FutureGet getAll(String keyString, String domainString) {
		return currentExecutingPeer().get(Number160.createHash(keyString)).domainKey(Number160.createHash(domainString)).all().start();
	}

	private PeerDHT currentExecutingPeer() {
		int index = this.currentExecutingPeerCounter;
		this.currentExecutingPeerCounter = (currentExecutingPeerCounter + 1) % peerDHTs.size();
		return peerDHTs.get(index);
	}

	@Override
	public void createTasks(Job job, List<FutureGet> procedureTaskFutureGetCollector, List<Task> procedureTaskCollector) {

		final String procedureDomain = DomainProvider.INSTANCE.jobProcedureDomain(job);
		final Number160 procedureDomainHash = Number160.createHash(procedureDomain);

		currentExecutingPeer().get(Number160.createHash(DomainProvider.PROCEDURE_KEYS)).domainKey(procedureDomainHash).all().start()
				.addListener(new BaseFutureListener<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							try {
								if (future.dataMap() != null) {
									for (Number640 n : future.dataMap().keySet()) {
										Object key = future.dataMap().get(n).object();
										// 1 key from procedure domain == 1 task
										procedureTaskFutureGetCollector.add(currentExecutingPeer().get(Number160.createHash(key.toString()))
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

			return this.currentExecutingPeer().add(Number160.createHash(keyString)).data(valueData).domainKey(Number160.createHash(domainString))
					.start();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public FuturePut put(String keyString, Object value, String domainString) {

		try {
			logger.info("put: Trying to perform: dHashtable.add(" + keyString + ", " + value + ").domain(" + domainString + ")");

			return this.currentExecutingPeer().put(Number160.createHash(keyString)).data(new Data(value))
					.domainKey(Number160.createHash(domainString)).start();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String taskExecutorDomain(Task task) {
		return DomainProvider.INSTANCE.executorTaskDomain(task, Tuple.create(owner(), 0));
	}

}
