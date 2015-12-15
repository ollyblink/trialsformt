package mapreduce.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.MRJobSubmissionManager;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.utils.IDCreator;
import mapreduce.utils.PortManager;
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

public class DHTUtils {
	private static final Logger logger = LoggerFactory.getLogger(DHTUtils.class);
	private static final int DEFAULT_NUMBER_OF_ADD_TRIALS = 3; // 3 times
	private static final long DEFAULT_TIME_TO_LIVE_IN_MS = 10000; // 10secs
	private PeerDHT peerDHT;
	private String bootstrapIP;
	private int bootstrapPort;
	private boolean isBootstrapper;
	private int port;
	private MRBroadcastHandler broadcastHandler;
	private String id;
	private String storageFilePath;
	/** How many times should the data be tried be added to the dht? */
	private int nrOfAddTrials;
	/** For how long should the job submitter wait until it declares the data adding to be failed? In milliseconds */
	private long timeToLiveInMs;

	public DHTUtils timeToLiveInMs(long timeToLiveInMs) {
		this.timeToLiveInMs = timeToLiveInMs;
		return this;
	}

	public DHTUtils nrOfAddTrials(int nrOfAddTrials) {
		this.nrOfAddTrials = nrOfAddTrials;
		return this;
	}

	private DHTUtils() {
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.broadcastHandler = new MRBroadcastHandler();
	}

	public static DHTUtils newInstance(String bootstrapIP, int bootstrapPort) {
		return new DHTUtils().bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort).port(PortManager.INSTANCE.generatePort())
				.timeToLiveInMs(DEFAULT_TIME_TO_LIVE_IN_MS).nrOfAddTrials(DEFAULT_NUMBER_OF_ADD_TRIALS);
	}

	/**
	 * Creates a BroadcastHandler and Peer and connects to the DHT. If a bootstrap port and ip were provided (meaning, there are already peers
	 * connected to a DHT), it will be bootstrap to that node.
	 * 
	 * @param performBlocking
	 * @return
	 */
	public void connect(boolean awaitUninterruptibly) {
		try {
			if (this.isBootstrapper) {
				this.port = this.bootstrapPort;
			}

			Peer peer = new PeerBuilder(Number160.createHash(this.id)).ports(this.port).broadcastHandler(this.broadcastHandler).start();

			if (!this.isBootstrapper) {
				this.doBootstrapping(peer, awaitUninterruptibly);
			}
			PeerBuilderDHT peerDHTBuilder = new PeerBuilderDHT(peer);

			if (this.storageFilePath != null) {
				peerDHTBuilder.storage(new StorageDisk(peer.peerID(), new File(this.storageFilePath), null));
			}
			this.peerDHT = peerDHTBuilder.start();
		} catch (IOException e) {
			logger.debug("Exception on bootstrapping", e);
		}
	}

	public void doBootstrapping(Peer peer, boolean awaitUninterruptibly) throws UnknownHostException {

		FutureBootstrap bootstrap = peer.bootstrap().inetAddress(InetAddress.getByName(bootstrapIP())).ports(bootstrapPort()).start();
		if (awaitUninterruptibly) {
			bootstrap.awaitUninterruptibly();
		} else {
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
		}
	}

	public void broadcastTask(Task task, IBCMessage message) {
		try {
			Number160 taskHash = Number160.createHash(task.id() + message.sender().toString() + message.status());
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			dataMap.put(new Number640(taskHash, taskHash, taskHash, taskHash), new Data(message));
			peerDHT.peer().broadcast(taskHash).dataMap(dataMap).start();
		} catch (IOException e) {
			logger.warn("Exception thrown in DHTConnectionProvider::broadcastTaskSchedule", e);
		}
	}

	public void broadcastJobUpdate(Job job, IBCMessage message) {
		try {
			Number160 jobHash = Number160.createHash(job.id() + message.sender().toString() + message.status());
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			dataMap.put(new Number640(jobHash, jobHash, jobHash, jobHash), new Data(message));
			peerDHT.peer().broadcast(jobHash).dataMap(dataMap).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addKVD(String keyString, Object value, String domainString, boolean asList, boolean awaitUninterruptibly,
			BaseFutureListener<FuturePut> listener) {
		try {
			logger.info("addKVD: Trying to perform: dHashtable.add(" + keyString + ", " + value + ").domain(" + domainString + ")");
			Number160 keyHash = Number160.createHash(keyString);
			Data valueData = new Data(value);
			if (asList) {
				valueData = new Data(new Value(value));
			}
			Number160 domainHash = Number160.createHash(domainString);

			FuturePut futurePut = this.peerDHT.add(keyHash).data(valueData).domainKey(domainHash).start();

			if (listener != null) {
				futurePut.addListener(listener);
			}
			if (awaitUninterruptibly) {
				futurePut.awaitUninterruptibly();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getKD(String keyString, Collection<Object> valueCollector, String domainString, boolean asList, boolean awaitUninterruptibly) {
		Number160 domainHash = Number160.createHash(domainString);
		Number160 keyHash = Number160.createHash(keyString);

		logger.info("getKVD: dHashtable.get(" + keyString + ").domain(" + domainString + ")");
		FutureGet getFuture = peerDHT.get(keyHash).domainKey(domainHash).all().start();

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
								synchronized (valueCollector) {
									valueCollector.add(valueObject);
									logger.info("getKVD: Successfully retrieved value for <K, Domain>: <" + keyString + ", " + domainString + ">: "
											+ valueObject);
								}
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

	}

	public void removeKD(String domainString, String keyString, boolean awaitUninterruptibly) {
		Number160 domainHash = Number160.createHash(domainString);
		Number160 keyHash = Number160.createHash(keyString);

		logger.info("removeKD: dHashtable.remove(" + keyString + ").domain(" + domainString + ")");
		FutureRemove futureRemove = peerDHT.remove(keyHash).domainKey(domainHash).all().start();
		futureRemove.addListener(new BaseFutureListener<FutureRemove>() {

			@Override
			public void operationComplete(FutureRemove future) throws Exception {
				if (future.isSuccess()) {
					logger.warn("removeKD: Successfully retrieved value for <K, Domain>: <" + keyString + ", " + domainString + ">");
				} else {
					logger.warn("removeKD: Failed trying to remove values for <K, Domain>: <" + keyString + ", " + domainString + ">");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("removeKD: Exception caught", t);
			}
		});
		if (awaitUninterruptibly) {
			futureRemove.awaitUninterruptibly();
		}

	}

	// GETTER/SETTER START
	// ======================
	public boolean isBootstrapper() {
		return isBootstrapper;
	}

	public DHTUtils isBootstrapper(boolean isBootstrapper) {
		this.isBootstrapper = isBootstrapper;
		return this;
	}

	public DHTUtils bootstrapIP(String bootstrapIP) {
		if (this.bootstrapIP == null) {// make sure it cannot be externally changed...
			this.bootstrapIP = bootstrapIP;
		}
		return this;
	}

	public String bootstrapIP() {
		return this.bootstrapIP;
	}

	public DHTUtils bootstrapPort(int bootstrapPort) {
		if (this.bootstrapPort == 0 && bootstrapPort > 0) {// make sure it cannot be externally changed...
			this.bootstrapPort = bootstrapPort;
		}
		return this;
	}

	public int bootstrapPort() {
		return this.bootstrapPort;
	}

	public DHTUtils port(int port) {
		if (this.port == 0 && port > 0) {// make sure it cannot be externally changed...
			this.port = port;
		}
		return this;
	}

	public int port() {
		return this.port;
	}

	public MRBroadcastHandler broadcastHandler() {
		return this.broadcastHandler;
	}

	public String storageFilePath() {
		return storageFilePath;
	}

	public DHTUtils storageFilePath(String storageFilePath) {
		this.storageFilePath = storageFilePath;
		return this;
	}

	public PeerAddress peerAddress() {
		return peerDHT.peerAddress();
	}

	// GETTER/SETTER FINISHED
	// ======================

	public void shutdown() {
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