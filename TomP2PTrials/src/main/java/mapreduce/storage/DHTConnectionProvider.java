package mapreduce.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.MRBroadcastHandler;
import mapreduce.execution.broadcasthandler.broadcastmessages.DistributedJobBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.ExecuteOrFinishedTaskMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.FinishedAllTasksBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.FinishedJobBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.exceptions.IncorrectFormatException;
import mapreduce.execution.exceptions.NotSetException;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.KeyValuePair;
import mapreduce.execution.jobtask.Task;
import mapreduce.utils.FormatUtils;
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

public class DHTConnectionProvider implements IDHTConnectionProvider {
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

	public static DHTConnectionProvider newDHTConnectionProvider() {
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
			Peer peer = new PeerBuilder(Number160.createHash("DHTConnectionProvider_"+RND.nextLong())).ports(port).broadcastHandler(this.broadcastHandler).start();
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
		IBCMessage message = DistributedJobBCMessage.newDistributedTaskBCMessage().job(job).sender(this.connectionPeer.peerAddress());
		broadcastJob(job, message);
	}

	@Override
	public void broadcastFinishedAllTasks(Job job) {
		IBCMessage message = FinishedAllTasksBCMessage.newFinishedAllTasksBCMessage().jobId(job.id()).tasks(job.tasks(job.currentProcedureIndex()))
				.sender(this.connectionPeer.peerAddress());

		// TODO: SYNCHRONIZE TASK RESULT LISTS
		broadcastJob(job, message);
	}

	@Override
	public void broadcastFinishedJob(Job job) {
		IBCMessage message = FinishedJobBCMessage.newFinishedJobBCMessage().jobId(job.id()).jobSubmitterId(job.jobSubmitterID())
				.sender(this.connectionPeer.peerAddress());
		broadcastJob(job, message);
	}

	private void broadcastJob(Job job, IBCMessage message) {
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
	public void broadcastFinishedTask(Task task) {
		broadcastTask(task, ExecuteOrFinishedTaskMessage.newFinishedTaskBCMessage().sender(this.connectionPeer.peerAddress()));
	}

	@Override
	public void broadcastTaskSchedule(Task task) {
		broadcastTask(task, ExecuteOrFinishedTaskMessage.newTaskAssignedBCMessage().sender(this.connectionPeer.peerAddress()));

	}

	private void broadcastTask(Task task, ExecuteOrFinishedTaskMessage message) {
		// task.updateExecutingPeerStatus(message.sender(), message.status());
		try {
			Number160 taskHash = Number160.createHash(task.id() + message.sender().toString() + message.status());
			message.taskId(task.id()).jobId(task.jobId()).sender(this.connectionPeer.peerAddress());
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			dataMap.put(new Number640(taskHash, taskHash, taskHash, taskHash), new Data(message));
			connectionPeer.peer().broadcast(taskHash).dataMap(dataMap).start();

		} catch (IOException e) {
			logger.warn("Exception thrown in DHTConnectionProvider::broadcastTaskSchedule", e);
		}
	}

	@Override
	public <KEY, VALUE> void addDataForTask(String taskId, final KEY key, final VALUE value) {
		try {
			KeyValuePair<KEY, VALUE> taskResultTupel = new KeyValuePair<KEY, VALUE>(key, value);
			FuturePut addData = this.connectionPeer.add(Number160.createHash(key.toString())).data(new Data(taskResultTupel))
					.domainKey(Number160.createHash(taskId)).versionKey(connectionPeer.peerID()).start();
			addData.addListener(new BaseFutureListener<FuturePut>() {

				@Override
				public void operationComplete(FuturePut future) throws Exception {
					if (future.isSuccess()) {
						logger.debug("Successfully added data for key " + key);
					} else {
						logger.error("Could not put data");
					}
				}

				@Override
				public void exceptionCaught(Throwable t) throws Exception {
					logger.debug("Exception caught", t);
				}

			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public <KEY, VALUE> List<KeyValuePair<KEY, VALUE>> getDataForTask(Task task) {
		final List<KeyValuePair<KEY, VALUE>> values = new ArrayList<KeyValuePair<KEY, VALUE>>();
		for (int i = 0; i < task.keys().size(); ++i) {
			FutureGet getFuture = connectionPeer.get(Number160.createHash(task.keys().get(i).toString())).domainKey(Number160.createHash(task.id()))
					.versionKey(connectionPeer.peerID()).all().start();
			getFuture.awaitUninterruptibly();
			if (getFuture.isSuccess()) {
				try {
					if (getFuture.data() != null) {
						values.add((KeyValuePair<KEY, VALUE>) getFuture.data().object());
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
		return values;
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

}
