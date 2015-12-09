package mapreduce.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.exceptions.IncorrectFormatException;
import mapreduce.execution.exceptions.NotSetException;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.manager.broadcasthandler.broadcastmessages.DistributedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.FinishedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.JobUpdateBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FormatUtils;
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
	private static final Random RND = new Random();

	private static final int MIN_PORT = 4001;
	private static final int PORT_RANGE = 1000;

	private int bootstrapPort;
	private String bootstrapIP;

	private PeerDHT connectionPeer;
	private int port;
	private MRBroadcastHandler broadcastHandler;
	private String storageFilePath;
	private boolean useDiskStorage;

	private DHTConnectionProvider() {
		this.broadcastHandler = new MRBroadcastHandler();
	}

	public static DHTConnectionProvider newInstance() {
		return new DHTConnectionProvider();
	}

	// GETTER/SETTER START
	// ======================

	public DHTConnectionProvider bootstrapIP(String bootstrapIP) {
		this.bootstrapIP = bootstrapIP;
		return this;
	}

	public String bootstrapIP() throws IncorrectFormatException, NotSetException {
		if (bootstrapIP != null) {
			if (FormatUtils.isCorrectIP4(bootstrapIP)) {
				return this.bootstrapIP;
			} else {
				throw new IncorrectFormatException("This IP has not the correct format.");
			}
		} else {
			throw new NotSetException("Could not find a valid IP.");
		}
	}

	public DHTConnectionProvider bootstrapPort(int bootstrapPort) {
		this.bootstrapPort = bootstrapPort;
		return this;
	}

	public int bootstrapPort() {
		return this.bootstrapPort;
	}

	public DHTConnectionProvider port(int port) {
		this.port = port;
		return this;
	}

	public int port() {
		if (port == 0) {
			this.port = MIN_PORT + RND.nextInt(PORT_RANGE);
		}
		return this.port;
	}

	@Override
	public MRBroadcastHandler broadcastHandler() {
		return this.broadcastHandler;
	}

	public String storageFilePath() {
		return storageFilePath;
	}

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
			this.port = port();
			Peer peer = new PeerBuilder(Number160.createHash("DHTConnectionProvider_" + RND.nextLong())).ports(port)
					.broadcastHandler(this.broadcastHandler).start();
			logger.warn("port: " + port);
			if (bootstrapIP != null && bootstrapPort > 0) {
				logger.warn(bootstrapIP + " " + bootstrapPort);
				doBootstrapping(peer);
			}
			PeerBuilderDHT peerDHTBuilder = new PeerBuilderDHT(peer);
			if (useDiskStorage()) {
				peerDHTBuilder.storage(new StorageDisk(peer.peerID(), new File(storageFilePath()), null));
			}
			this.connectionPeer = peerDHTBuilder.start();
		} catch (IOException e) {
			logger.debug("Exception on bootstrapping", e);
		}
		return this;
	}

	private void doBootstrapping(Peer peer) throws UnknownHostException {
		try {
			FutureBootstrap bootstrap = peer.bootstrap().inetAddress(InetAddress.getByName(bootstrapIP())).ports(bootstrapPort()).start();
			bootstrap.addListener(new BaseFutureListener<FutureBootstrap>() {

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
		} catch (IncorrectFormatException e) {
			e.printStackTrace();
		} catch (NotSetException e) {
			e.printStackTrace();
		}
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
		try {

			String domain = DomainProvider.INSTANCE.domain(task, connectionPeer.peerAddress());

			Number160 domainKey = Number160.createHash(domain);
			Number160 keyHash = Number160.createHash(key.toString());
			logger.info("addTaskData: Domain: " + domain);
			logger.info("addTaskData: Domainkey: " + domainKey);
			logger.info("addTaskData: Key:" + keyHash);

			FuturePut await = this.connectionPeer.add(keyHash).data(new Data(new Value(value))).domainKey(domainKey).start();
			await.addListener(new BaseFutureListener<FuturePut>() {

				@Override
				public void operationComplete(FuturePut future) throws Exception {
					if (future.isSuccess()) {
						logger.info("Successfully added data for key " + key);
					} else {
						logger.error("Could not put data");
					}
				}

				@Override
				public void exceptionCaught(Throwable t) throws Exception {
					logger.info("Exception caught", t);
				}

			});

			if (awaitUninterruptibly) {
				await.awaitUninterruptibly();
			}
			addTaskKey(task, key, awaitUninterruptibly);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void addTaskKey(final Task task, final Object key, boolean awaitUninterruptibly) {
		try {
			String domain = DomainProvider.INSTANCE.domain(task, connectionPeer.peerAddress());

			Number160 domainKey = Number160.createHash(domain);

			Number160 keyLocationHash = Number160.createHash(KEY_LOCATION_PREAMBLE + domain);

			FuturePut await = this.connectionPeer.add(keyLocationHash).data(new Data(key.toString())).domainKey(domainKey).start();

			await.addListener(new BaseFutureListener<FuturePut>() {

				@Override
				public void operationComplete(FuturePut future) throws Exception {
					if (future.isSuccess()) {
						logger.info("Successfully added key " + key);
					} else {
						logger.error("Could not put key " + key);
					}
				}

				@Override
				public void exceptionCaught(Throwable t) throws Exception {
					logger.debug("Exception caught", t);
				}

			});
			if (awaitUninterruptibly) {
				await.awaitUninterruptibly();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Multimap<Object, Object> getTaskData(Task task, LocationBean locationBean) {
		final Multimap<Object, Object> taskKeyValues = ArrayListMultimap.create();

		// String domain = domain(task, task.finalPeerAddress());
		Number160 domainKey = Number160.createHash(locationBean.domain(task.id()));
		logger.info("getTaskData: Domain: " + locationBean.domain(task.id()));
		logger.info("getTaskData: Domainkey: " + domainKey);
		List<Object> taskKeys = getTaskKeys(task, locationBean);
		logger.info("getTaskData: taskkeys: " + taskKeys);

		for (int i = 0; i < taskKeys.size(); ++i) {
			String key = taskKeys.get(i).toString();
			FutureGet getFuture = connectionPeer.get(Number160.createHash(key)).domainKey(domainKey).all().start();
			getFuture.awaitUninterruptibly();
			if (getFuture.isSuccess()) {
				try {
					if (getFuture.dataMap() != null) {
						for (Number640 n : getFuture.dataMap().keySet()) {
							Value kvPair = (Value) getFuture.dataMap().get(n).object();
							taskKeyValues.put(key, kvPair.value());
						}
					} else {
						logger.warn("future data is null!");
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return taskKeyValues;
	}

	@Override
	public List<Object> getTaskKeys(Task task, LocationBean locationBean) {
		List<Object> keys = new ArrayList<Object>();
		// String domain = domain(task, task.finalPeerAddress());
		String domain = locationBean.domain(task.id());
		Number160 domainKey = Number160.createHash(domain);
		Number160 keyLocationHash = Number160.createHash(KEY_LOCATION_PREAMBLE + domain);
		FutureGet getFuture = connectionPeer.get(keyLocationHash).domainKey(domainKey).all().start();
		getFuture.awaitUninterruptibly();
		if (getFuture.isSuccess()) {
			try {
				if (getFuture.data() != null) {
					Collection<Data> values = getFuture.dataMap().values();
					for (Data data : values) {
						keys.add(data.object());
					}
				} else {
					logger.warn("future task key data is null!");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return keys;
	}

	@Override
	public void removeTaskResultsFor(Task task, LocationBean locationBean) {
		List<Object> keys = getTaskKeys(task, locationBean);
		// String domain = domain(task, task.finalPeerAddress());
		String domain = locationBean.domain(task.id());
		Number160 domainKey = Number160.createHash(domain);
		for (final Object key : keys) {
			connectionPeer.remove(Number160.createHash(key.toString())).domainKey(domainKey).all().start()
					.addListener(new BaseFutureListener<FutureRemove>() {

						@Override
						public void operationComplete(FutureRemove future) throws Exception {
							if (future.isSuccess()) {
								logger.warn("Successfully removed data");
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
	public void removeTaskKeysFor(Task task, LocationBean locationBean) {

		String domain = locationBean.domain(task.id());
		Number160 domainKey = Number160.createHash(domain);
		Number160 keyLocationHash = Number160.createHash(KEY_LOCATION_PREAMBLE + domain);
		connectionPeer.remove(keyLocationHash).domainKey(domainKey).all().start().addListener(new BaseFutureListener<FutureRemove>() {

			@Override
			public void operationComplete(FutureRemove removeFuture) throws Exception {
				if (removeFuture.isSuccess()) {
					logger.warn("Successfully removed data");
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
	public DHTConnectionProvider useDiskStorage(boolean useDiskStorage) {
		this.useDiskStorage = useDiskStorage;
		return this;
	}

	@Override
	public boolean useDiskStorage() {
		return useDiskStorage;
	}

	@Override
	public String peerAddressString() {
		return connectionPeer.peerAddress().inetAddress() + ":" + connectionPeer.peerAddress().tcpPort();
	}

	@Override
	public PeerAddress peerAddress() {
		return connectionPeer.peerAddress();
	}

}
