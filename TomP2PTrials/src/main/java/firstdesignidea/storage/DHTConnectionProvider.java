package firstdesignidea.storage;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.broadcasthandler.MRBroadcastHandler;
import firstdesignidea.execution.broadcasthandler.broadcastmessages.DistributedTaskBCMessage;
import firstdesignidea.execution.exceptions.IncorrectFormatException;
import firstdesignidea.execution.exceptions.NotSetException;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.utils.FormatUtils;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class DHTConnectionProvider {
	private static Logger logger = LoggerFactory.getLogger(DHTConnectionProvider.class);
	private static final Random RND = new Random();

	private static final int MIN_PORT = 4001;
	private static final int PORT_RANGE = 100;

	private int bootstrapPort;
	private String bootstrapIP;

	private PeerDHT connectionPeer;
	private int port;
	private MRBroadcastHandler broadcastHandler;

	private DHTConnectionProvider() {
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

	// GETTER/SETTER FINISHED
	// ======================

	/**
	 * Creates a BroadcastHandler and Peer and connects to the DHT. If a bootstrap port and ip were provided (meaning, there are already peers
	 * connected to a DHT), it will be bootstrap to that node.
	 */
	public void connect() {
		try {
			this.port = port();
			Peer peer = new PeerBuilder(Number160.createHash(RND.nextLong())).ports(port).broadcastHandler(this.broadcastHandler).start();
			logger.warn("port: " + port);
			if (bootstrapIP != null && bootstrapPort > 0) {
				logger.warn(bootstrapIP + " " + bootstrapPort);
				doBootstrapping(peer);
			}

			this.connectionPeer = new PeerBuilderDHT(peer).start();
		} catch (IOException e) {
			logger.debug("Exception on bootstrapping", e);
		}
	}

	private void doBootstrapping(Peer peer) throws UnknownHostException {
		try {
			FutureBootstrap bootstrap = peer.bootstrap().inetAddress(InetAddress.getByName(bootstrapIP())).ports(bootstrapPort()).start();
			bootstrap.addListener(new BaseFutureListener<FutureBootstrap>() {

				@Override
				public void operationComplete(FutureBootstrap future) throws Exception {
					if (future.isSuccess()) {
						logger.debug("successfully bootstrapped to " + bootstrapIP + "/" + bootstrapPort);
					} else {
						logger.debug("No success on bootstrapping: fail reason: " + future.failedReason());
					}
				}

				@Override
				public void exceptionCaught(Throwable t) throws Exception {
					logger.debug("Exception on bootstrapping", t);
				}
			});
		} catch (IncorrectFormatException e) {
			e.printStackTrace();
		} catch (NotSetException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Adds a new task to the DHT with key made from task.id() and domainKey made from task.jobId()
	 * 
	 * @param task
	 */
	public void addTask(Task task) {
		try {
			Number160 taskHash = Number160.createHash(task.id());
			Number160 jobHash = Number160.createHash(task.jobId());
			FuturePut put = this.connectionPeer.put(taskHash).domainKey(jobHash).data(new Data(task)).start();
			put.addListener(newTaskListener(task.id(), task.jobId()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private BaseFutureListener<FuturePut> newTaskListener(final String taskId, final String jobId) {
		return new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					DistributedTaskBCMessage message = DistributedTaskBCMessage.newDistributedTaskBCMessage().jobId(jobId).taskId(taskId);
					Number160 taskHash = Number160.createHash(taskId);
					Number160 jobHash = Number160.createHash(jobId);
					NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
					dataMap.put(new Number640(taskHash, jobHash, taskHash, taskHash), new Data(message));
					connectionPeer.peer().broadcast(taskHash).dataMap(dataMap).start();

				} else {
					logger.error("Could not put new job into job queue");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("Exception caught in putNewJobListener", t);
			}

		};
	}

	public Task getTask(String taskId, String jobId) {

		Number160 taskHash = Number160.createHash(taskId);
		Number160 jobDomainKey = Number160.createHash(jobId);
		FutureGet getTask = this.connectionPeer.get(taskHash).domainKey(jobDomainKey).start();
		getTask.awaitUninterruptibly();
		if (getTask.isSuccess()) {
			if (getTask.data() != null) {
				try {
					return (Task) getTask.data().object();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	public void addJob(Job job) {
		try {
			Number160 jobHash = Number160.createHash(job.id());
			FuturePut put = this.connectionPeer.put(jobHash).domainKey(jobHash).data(new Data(job)).start();
			put.addListener(newJobListener(job.id()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private BaseFutureListener<? extends BaseFuture> newJobListener(final String jobId) {
		return new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					Number160 jobHash = Number160.createHash(jobId);
					NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
					dataMap.put(new Number640(jobHash, jobHash, jobHash, jobHash), new Data(jobHash));
					connectionPeer.peer().broadcast(jobHash).dataMap(dataMap).start();
				} else {
					logger.error("Could not put new job into job queue");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("Exception caught in putNewJobListener", t);
			}

		};
	}

	public Job getJob(String jobId) {
		Number160 jobHash = Number160.createHash(jobId);
		FutureGet getJob = this.connectionPeer.get(jobHash).domainKey(jobHash).start();
		getJob.awaitUninterruptibly();
		if (getJob.isSuccess()) {
			if (getJob.data() != null) {
				try {
					return (Job) getJob.data().object();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	public IBroadcastDistributor broadcastDistributor() {
		return this.broadcastHandler;
	}

	public DHTConnectionProvider broadcastDistributor(MRBroadcastHandler broadcastHandler) {
		this.broadcastHandler = broadcastHandler;
		return this;
	}

}
