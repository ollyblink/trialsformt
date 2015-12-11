package mapreduce.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.manager.broadcasthandler.broadcastmessages.DistributedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.FinishedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.JobUpdateBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.IDCreator;
import mapreduce.utils.PortManager;
import mapreduce.utils.Tuple;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.FutureRemove;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageDisk;

public class DHTConnectionProvider implements IDHTConnectionProvider {
	private static final String KEY_LOCATION_PREAMBLE = "KEYS_FOR_";
	private static Logger logger = LoggerFactory.getLogger(DHTConnectionProvider.class);

	private String id;
	private String bootstrapIP;
	private int bootstrapPort;

	private PeerDHT connectionPeer;
	private int port;
	private MRBroadcastHandler broadcastHandler;
	private String storageFilePath;
	private boolean isBootstrapper;

	private DHTConnectionProvider() {
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.broadcastHandler = new MRBroadcastHandler();
	}

	public static DHTConnectionProvider newInstance(String bootstrapIP, int bootstrapPort) {
		DHTConnectionProvider provider = new DHTConnectionProvider().bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort)
				.port(PortManager.INSTANCE.generatePort());

		return provider;
	}

	// GETTER/SETTER START
	// ======================
	@Override
	public boolean isBootstrapper() {
		return isBootstrapper;
	}

	@Override
	public DHTConnectionProvider isBootstrapper(boolean isBootstrapper) {
		this.isBootstrapper = isBootstrapper;
		return this;
	}

	@Override
	public DHTConnectionProvider bootstrapIP(String bootstrapIP) {
		if (this.bootstrapIP == null) {// make sure it cannot be externally changed...
			this.bootstrapIP = bootstrapIP;
		}
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

	public DHTConnectionProvider port(int port) {
		if (this.port == 0 && port > 0) {// make sure it cannot be externally changed...
			this.port = port;
		}
		return this;
	}

	public int port() {
		return this.port;
	}

	@Override
	public MRBroadcastHandler broadcastHandler() {
		return this.broadcastHandler;
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

	/**
	 * Creates a BroadcastHandler and Peer and connects to the DHT. If a bootstrap port and ip were provided (meaning, there are already peers
	 * connected to a DHT), it will be bootstrap to that node.
	 */
	@Override
	public DHTConnectionProvider connect() {
		try {
			if (isBootstrapper) {
				this.port = this.bootstrapPort;
			}

			Peer peer = new PeerBuilder(Number160.createHash(id)).ports(port).broadcastHandler(this.broadcastHandler).start();

			if (!isBootstrapper) {
				doBootstrapping(peer);
			}
			PeerBuilderDHT peerDHTBuilder = new PeerBuilderDHT(peer);

			if (this.storageFilePath != null) {
				peerDHTBuilder.storage(new StorageDisk(peer.peerID(), new File(this.storageFilePath), null));
			}
			this.connectionPeer = peerDHTBuilder.start();
		} catch (IOException e) {
			logger.debug("Exception on bootstrapping", e);
		}
		return this;
	}

	private void doBootstrapping(Peer peer) throws UnknownHostException {

		FutureBootstrap bootstrap = peer.bootstrap().inetAddress(InetAddress.getByName(bootstrapIP())).ports(bootstrapPort()).start();
		bootstrap.awaitUninterruptibly();
		// bootstrap.addListener(new BaseFutureListener<FutureBootstrap>() {
		//
		// @Override
		// public void operationComplete(FutureBootstrap future) throws Exception {
		// if (future.isSuccess()) {
		// logger.warn("successfully bootstrapped to " + bootstrapIP + "/" + bootstrapPort);
		// } else {
		// logger.warn("No success on bootstrapping: fail reason: " + future.failedReason());
		// }
		// }
		//
		// @Override
		// public void exceptionCaught(Throwable t) throws Exception {
		// logger.warn("Exception on bootstrapping", t);
		// }
		// });
	}

	public void storage() {

		// StorageLayer storageLayer = this.connectionPeer.storageLayer();
		// Number640 key = new Number640(locationKey, domainKey, contentKey, versionKey);
		// storageLayer.contains(key);
	}

	@Override
	public void broadcastNewJob(Job job) {
		IBCMessage message = DistributedJobBCMessage.newInstance().job(job).sender(this.connectionPeer.peerAddress());
		broadcastJobUpdate(job, message);
	}

	@Override
	public void broadcastFinishedAllTasks(Job job) {
		broadcastJobUpdate(job, JobUpdateBCMessage.newFinishedAllTasksBCMessage().job(job).sender(this.connectionPeer.peerAddress()));

	}

	@Override
	public void broadcastFinishedJob(Job job) {
		broadcastJobUpdate(job,
				FinishedJobBCMessage.newInstance().jobSubmitterId(job.jobSubmitterID()).job(job).sender(this.connectionPeer.peerAddress()));
	}

	private void broadcastJobUpdate(Job job, IBCMessage message) {
		try {
			Number160 jobHash = Number160.createHash(job.id() + message.sender().toString() + message.status());
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			dataMap.put(new Number640(jobHash, jobHash, jobHash, jobHash), new Data(message));
			connectionPeer.peer().broadcast(jobHash).dataMap(dataMap).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void broadcastExecutingTask(Task task) {
		broadcastTask(task, TaskUpdateBCMessage.newExecutingTaskInstance().task(task).sender(this.connectionPeer.peerAddress()));

	}

	@Override
	public void broadcastFinishedTask(Task task, Number160 resultHash) {
		broadcastTask(task,
				TaskUpdateBCMessage.newFinishedTaskInstance().resultHash(resultHash).task(task).sender(this.connectionPeer.peerAddress()));
	}

	private void broadcastTask(Task task, IBCMessage message) {
		try {
			Number160 taskHash = Number160.createHash(task.id() + message.sender().toString() + message.status());
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			dataMap.put(new Number640(taskHash, taskHash, taskHash, taskHash), new Data(message));
			connectionPeer.peer().broadcast(taskHash).dataMap(dataMap).start();
		} catch (IOException e) {
			logger.warn("Exception thrown in DHTConnectionProvider::broadcastTaskSchedule", e);
		}
	}

	@Override
	public void addTaskData(Task task, final Object key, final Object value, boolean awaitUninterruptibly) {
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, connectionPeer.peerAddress());
		String keyString = key.toString();

		addKVD(keyString, value, domainString, true, awaitUninterruptibly);
		addTaskKey(task, key, awaitUninterruptibly);
	}

	@Override
	public void addTaskKey(Task task, Object key, boolean awaitUninterruptibly) {
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, connectionPeer.peerAddress());
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		Object value = key;

		addKVD(keyString, value, domainString, false, awaitUninterruptibly);
	}

	@Override
	public Multimap<Object, Object> getTaskData(Task task, Tuple<PeerAddress, Integer> selectedExecutor, boolean awaitUninterruptibly) {
		final Multimap<Object, Object> taskKeyValues = ArrayListMultimap.create();
		Set<Object> taskKeys = getTaskKeys(task, selectedExecutor, awaitUninterruptibly);

		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		boolean asList = true;
		for (Object key : taskKeys) {
			String keyString = key.toString();
			List<Object> values = getKVD(keyString, domainString, asList, awaitUninterruptibly);
			taskKeyValues.putAll(key, values);
		}
		return taskKeyValues;
	}

	@Override
	public Set<Object> getTaskKeys(Task task, Tuple<PeerAddress, Integer> selectedExecutor, boolean awaitUninterruptibly) {
		Set<Object> keys = new HashSet<Object>();
		String domainString = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		boolean asList = false;

		List<Object> keysList = getKVD(keyString, domainString, asList, awaitUninterruptibly);
		keys.addAll(keysList);

		return keys;
	}

	@Override
	public void addProcedureTaskPeerDomain(Task task, Object key, Tuple<PeerAddress, Integer> selectedExecutor, boolean awaitUninterruptibly) {
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(task.jobId(), task.procedure().getClass().getSimpleName(),
				task.procedureIndex());
		String keyString = key.toString();
		String value = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());

		addKVD(keyString, value, domainString, false, awaitUninterruptibly);
		addProcedureKey(task, key, awaitUninterruptibly);
	}

	@Override
	public void addProcedureKey(Task task, Object key, boolean awaitUninterruptibly) {
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(task.jobId(), task.procedure().getClass().getSimpleName(),
				task.procedureIndex());
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		Object value = key;

		addKVD(keyString, value, domainString, false, awaitUninterruptibly);
	}

	@Override
	public Set<Object> getProcedureKeys(Job job, boolean awaitUninterruptibly) {
		Set<Object> keys = new HashSet<Object>();
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
		String keyString = KEY_LOCATION_PREAMBLE + domainString;
		boolean asList = false;
		List<Object> keysList = getKVD(keyString, domainString, asList, awaitUninterruptibly);
		keys.addAll(keysList);

		return keys;
	}

	@Override
	public Set<Object> getProcedureTaskPeerDomains(Job job, Object key, boolean awaitUninterruptibly) {
		Set<Object> domains = new HashSet<Object>();
		String domainString = DomainProvider.INSTANCE.jobProcedureDomain(job);
		String keyString = key.toString();
		boolean asList = false;

		List<Object> keysList = getKVD(domainString, keyString, asList, awaitUninterruptibly);
		domains.addAll(keysList);

		return domains;
	}

	private void addKVD(String keyString, Object value, String domainString, boolean asList, boolean awaitUninterruptibly) {
		try {
			logger.info("addKVD: Trying to perform: dHashtable.add(" + keyString + ", " + value + ").domain(" + domainString + ")");
			Number160 keyHash = Number160.createHash(keyString);
			Data valueData = new Data(value);
			if (asList) {
				valueData = new Data(new Value(value));
			}
			Number160 domainHash = Number160.createHash(domainString);

			FuturePut futurePut = this.connectionPeer.add(keyHash).data(valueData).domainKey(domainHash).start();

			futurePut.addListener(new BaseFutureListener<FuturePut>() {

				@Override
				public void operationComplete(FuturePut future) throws Exception {
					if (future.isSuccess()) {
						logger.info("Successfully added <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
					} else {
						logger.error("Failed tyring to add <K, V, Domain>: <" + keyString + ", " + value + ", " + domainString + ">");
					}
				}

				@Override
				public void exceptionCaught(Throwable t) throws Exception {
					logger.debug("Exception caught", t);
				}

			});
			if (awaitUninterruptibly) {
				futurePut.awaitUninterruptibly();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private List<Object> getKVD(String keyString, String domainString, boolean asList, boolean awaitUninterruptibly) {
		Number160 domainHash = Number160.createHash(domainString);
		Number160 keyHash = Number160.createHash(keyString);
		List<Object> values = new ArrayList<Object>();

		logger.info("getKVD: dHashtable.get(" + keyString + ").domain(" + domainString + ")");
		FutureGet getFuture = connectionPeer.get(keyHash).domainKey(domainHash).all().start();

		if (awaitUninterruptibly) {
			getFuture.awaitUninterruptibly();
		}
		getFuture.addListener(new BaseFutureListener<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					try {
						if (getFuture.dataMap() != null) {
							for (Number640 n : getFuture.dataMap().keySet()) {
								Object valueObject = null;
								if (asList) {
									Value value = (Value) getFuture.dataMap().get(n).object();
									valueObject = value.value();
								} else {
									valueObject = getFuture.dataMap().get(n).object();
								}
								values.add(valueObject);
								logger.info("getKVD: Successfully retrieved value for <K, Domain>: <" + keyString + ", " + domainString + ">: "
										+ valueObject);
							}
						} else {
							logger.warn("getKVD: Value for <K, Domain>: <" + keyString + ", " + domainString + "> is null!");
						}
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} else {
					logger.error("getKVD: Failed trying to retrieve value for <K, Domain>: <" + keyString + ", " + domainString + ">");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("getKVD: Exception caught", t);

			}
		});

		return values;
	}

	@Override
	public void removeTaskResultsFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor, boolean awaitUninterruptibly) {
		Set<Object> keys = getTaskKeys(task, selectedExecutor, awaitUninterruptibly);
		String domain = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		Number160 domainKey = Number160.createHash(domain);

		for (final Object key : keys) {
			logger.info("removeTaskResultsFor: dHashtable.remove(" + key + ").domain(" + domain + ")");
			connectionPeer.remove(Number160.createHash(key.toString())).domainKey(domainKey).all().start().awaitUninterruptibly()
					.addListener(new BaseFutureListener<FutureRemove>() {

						@Override
						public void operationComplete(FutureRemove future) throws Exception {
							if (future.isSuccess()) {
								logger.warn("Successfully removed data for key " + key);
							} else {
								logger.warn("No success on trying to remove data for key " + key + ".");
							}
						}

						@Override
						public void exceptionCaught(Throwable t) throws Exception {
							logger.debug("Exception caught", t);
						}
					}

			);
		}

	}

	@Override
	public void removeTaskKeysFor(Task task, Tuple<PeerAddress, Integer> selectedExecutor, boolean awaitUninterruptibly) {

		String domain = DomainProvider.INSTANCE.taskPeerDomain(task, selectedExecutor.first(), selectedExecutor.second());
		String keyLocation = KEY_LOCATION_PREAMBLE + domain;
		Number160 domainKey = Number160.createHash(domain);
		Number160 keyLocationHash = Number160.createHash(keyLocation);

		logger.info("removeTaskKeysFor: dHashtable.remove(" + keyLocation + ").domain(" + domain + ")");
		connectionPeer.remove(keyLocationHash).domainKey(domainKey).all().start().awaitUninterruptibly()
				.addListener(new BaseFutureListener<FutureRemove>() {

					@Override
					public void operationComplete(FutureRemove removeFuture) throws Exception {
						if (removeFuture.isSuccess()) {
							logger.warn("Successfully removed keys");
						} else {
							logger.warn("Something wrong trying to remove keys.");
						}
					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						logger.debug("Exception caught", t);

					}
				}

		);
	}

	@Override
	public boolean alreadyExecuted(Task task) {
		return (task.statiForPeer(this.connectionPeer.peerAddress()) != null);
	}

	@Override
	public void shutdown() {
		BaseFuture shutdown = connectionPeer.shutdown();
		shutdown.addListener(new BaseFutureListener<BaseFuture>() {

			@Override
			public void operationComplete(BaseFuture future) throws Exception {
				if (future.isSuccess()) {
					logger.trace("Successfully shut down peer " + connectionPeer.peerID() + ".");
				} else {

					logger.trace("Could not shut down peer " + connectionPeer.peerID() + ".");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.warn("Exception thrown in DHTConnectionProvider::shutdown()", t);
			}
		});
	}

	@Override
	public PeerAddress peerAddress() {
		return connectionPeer.peerAddress();
	}

}
