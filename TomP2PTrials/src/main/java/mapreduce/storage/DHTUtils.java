package mapreduce.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.MRBroadcastHandler;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
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

public class DHTUtils {
	private static final Logger logger = LoggerFactory.getLogger(DHTUtils.class);
	private PeerDHT peerDHT;
	private String bootstrapIP;
	private int bootstrapPort;
	private boolean isBootstrapper;
	private int port;
	private MRBroadcastHandler broadcastHandler;
	private String id;
	private String storageFilePath;

	private DHTUtils() {
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.broadcastHandler = new MRBroadcastHandler();
	}

	public static DHTUtils newInstance(String bootstrapIP, int bootstrapPort) {
		return new DHTUtils().bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort).port(PortManager.INSTANCE.generatePort());
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

	public void broadcastTaskUpdate(Task task, IBCMessage message) {
		try {
			Number160 taskHash = Number160.createHash(DomainProvider.INSTANCE.executorTaskDomain(task,
					Tuple.create(peerDHT.peerAddress(), task.executingPeers().get(peerDHT.peerAddress()).size() - 1)));
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

	public FuturePut addKVD(String keyString, Object value, String domainString, boolean asList) {
		FuturePut futurePut = null;
		try {
			logger.info("addKVD: Trying to perform: dHashtable.add(" + keyString + ", " + value + ").domain(" + domainString + ")");
			Number160 keyHash = Number160.createHash(keyString);
			Data valueData = new Data(value);
			if (asList) {
				valueData = new Data(new Value(value));
			}
			Number160 domainHash = Number160.createHash(domainString);

			futurePut = this.peerDHT.add(keyHash).data(valueData).domainKey(domainHash).start();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return futurePut;
	}

	public FutureGet getKD(String keyString, String domainString) {
		Number160 domainHash = Number160.createHash(domainString);
		Number160 keyHash = Number160.createHash(keyString);

		logger.info("getKVD: dHashtable.get(" + keyString + ").domain(" + domainString + ")");
		return peerDHT.get(keyHash).domainKey(domainHash).all().start();

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