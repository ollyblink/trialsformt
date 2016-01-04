package mapreduce.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasting.MRBroadcastHandler;
import mapreduce.manager.broadcasting.broadcastmessages.CompletedBCMessage;
import mapreduce.manager.broadcasting.broadcastmessages.IBCMessage;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;
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
	private MRBroadcastHandler broadcastHandler;
	private String owner;
	private String bootstrapIP;
	private int bootstrapPort;
	private boolean isBootstrapper;
	private int port;
	private String id;
	private String storageFilePath;
	private int numberOfPeers = DEFAULT_NUMBER_OF_PEERS;
	private TreeMap<Job, PriorityBlockingQueue<IBCMessage>> jobs;
	private int currentExecutingPeerCounter = 0;

	private DHTConnectionProvider() {

	}

	private DHTConnectionProvider(String bootstrapIP, int bootstrapPort) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.bootstrapIP = bootstrapIP;
		this.bootstrapPort = bootstrapPort;
		this.peerDHTs = SyncedCollectionProvider.syncedArrayList();
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

	public DHTConnectionProvider externalPeers(List<PeerDHT> peerDHTs, MRBroadcastHandler bcHandler) {
		this.peerDHTs = peerDHTs;
		this.broadcastHandler = bcHandler;
		return this;
	}

	@Override
	public boolean isBootstrapper() {
		return this.isBootstrapper;
	}

	@Override
	public DHTConnectionProvider jobQueues(TreeMap<Job, PriorityBlockingQueue<IBCMessage>> jobs) {
		this.jobs = jobs;

		if (this.broadcastHandler != null) {
			this.broadcastHandler.jobQueues(jobs);
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
		if (broadcastHandler == null) {
			this.broadcastHandler = MRBroadcastHandler.create().dhtConnectionProvider(this).jobQueues(jobs);
		}

		for (int i = 0; i < this.numberOfPeers; ++i) {

			try {
				if (this.isBootstrapper && bootstrapper == 0) {
					this.port = this.bootstrapPort;
				} else {
					this.port = this.bootstrapPort++;
				}
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
	//
	// @Override
	// public JobDistributedBCMessage broadcastNewJob(Job job) {
	// JobDistributedBCMessage message = JobDistributedBCMessage.newInstance().job(job).sender(this.owner());
	// broadcastJobUpdate(job, message);
	// return message;
	// }
	//
	// // @Override
	// // public JobFailedBCMessage broadcastFailedJob(Job job) {
	// // JobFailedBCMessage message = JobFailedBCMessage.newInstance().job(job).sender(this.owner());
	// // broadcastJobUpdate(job, message);
	// // return message;
	// // }
	//
	// @Override
	// public ProcedureCompletedBCMessage broadcastFinishedAllTasksOfProcedure(Job job) {
	// ProcedureCompletedBCMessage message = ProcedureCompletedBCMessage.create().job(job).sender(this.owner());
	// broadcastJobUpdate(job, message);
	// return message;
	//
	// }
	//
	// @Override
	// public JobFinishedBCMessage broadcastFinishedJob(Job job) {
	// JobFinishedBCMessage message = JobFinishedBCMessage.newInstance().job(job).sender(this.owner());
	// broadcastJobUpdate(job, message);
	// return message;
	// }
	//
	// @Override
	// public TaskCompletedBCMessage broadcastExecutingTask(Task task, Tuple<String, Integer> taskExecutor) {
	// TaskCompletedBCMessage message = TaskCompletedBCMessage.createTaskExecutingBCMessage().task(task).sender(taskExecutor.first());
	// broadcastTaskUpdate(task, taskExecutor, message);
	// return message;
	// }
	//
	// @Override
	// public TaskCompletedBCMessage broadcastFinishedTask(Task task, Tuple<String, Integer> taskExecutor, Number160 resultHash) {
	// TaskCompletedBCMessage message = TaskCompletedBCMessage.createTaskFinishedBCMessage().task(task).sender(taskExecutor.first())
	// .resultHash(resultHash);
	// broadcastTaskUpdate(task, taskExecutor, message);
	// return message;
	// }

	// @Override
	// public TaskUpdateBCMessage broadcastFailedTask(Task task) {
	// TaskUpdateBCMessage message = TaskUpdateBCMessage.newFailedTaskInstance().task(task).sender(this.owner());
	// broadcastTaskUpdate(task, message);
	// return message;
	// }

	@Override
	public void broadcastCompletion(CompletedBCMessage completedMessage) {
		Number160 bcHash = Number160.createHash(completedMessage.toString());
		NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
		try {
			dataMap.put(new Number640(bcHash, bcHash, bcHash, bcHash), new Data(completedMessage));
		} catch (IOException e) {
			e.printStackTrace();
		}
		currentExecutingPeer().peer().broadcast(bcHash).dataMap(dataMap).start();

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
		if (this.broadcastHandler != null) {
			this.broadcastHandler.owner(owner);
		}
		return this;
	}

	@Override
	public FutureGet getAll(String keyString, String domainString) {
		return currentExecutingPeer().get(Number160.createHash(keyString)).domainKey(Number160.createHash(domainString)).all().start();
	}

	@Override
	public FutureGet get(String keyString, String domainString) {
		return currentExecutingPeer().get(Number160.createHash(keyString)).domainKey(Number160.createHash(domainString)).start();
	}

	private PeerDHT currentExecutingPeer() {
		int index = this.currentExecutingPeerCounter;
		this.currentExecutingPeerCounter = (currentExecutingPeerCounter + 1) % peerDHTs.size();
		return peerDHTs.get(index);
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
	public FuturePut addAll(String keyString, Collection<Data> values, String domainString) { 
		return this.currentExecutingPeer().add(Number160.createHash(keyString)).dataSet(values).domainKey(Number160.createHash(domainString)).start();
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
	public MRBroadcastHandler broadcastHandler() {
		return broadcastHandler;
	}

}
