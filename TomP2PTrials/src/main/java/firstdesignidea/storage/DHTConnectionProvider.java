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
import firstdesignidea.execution.computation.context.IContext;
import firstdesignidea.execution.exceptions.IncorrectFormatException;
import firstdesignidea.execution.exceptions.NotSetException;
import firstdesignidea.execution.jobtask.IJobManager;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.utils.FormatUtils;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class DHTConnectionProvider {
	private static Logger logger = LoggerFactory.getLogger(DHTConnectionProvider.class);
	private static final Random RND = new Random(42L);

	private static final int MIN_PORT = 4000;
	private static final int PORT_RANGE = 10000;

	private int bootstrapPort;
	private String bootstrapIP;

	private PeerDHT connectionPeer;
	private int port;
	private IJobManager jobManager;

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

	public DHTConnectionProvider jobManager(IJobManager jobManager) {
		this.jobManager = jobManager;
		return this;
	}

	public IJobManager jobManager() {
		return this.jobManager;
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
			return MIN_PORT + RND.nextInt(PORT_RANGE);
		} else {
			return this.port;
		}
	}

	// GETTER/SETTER FINISHED
	// ======================

	/**
	 * Creates a BroadcastHandler and Peer and connects to the DHT. If a bootstrap port and ip were provided (meaning, there are already peers
	 * connected to a DHT), it will be bootstrap to that node.
	 */
	public void connect() {
		try {
			MRBroadcastHandler broadcastHandler = new MRBroadcastHandler(jobManager);
			Peer peer = new PeerBuilder(Number160.createHash(RND.nextLong())).ports(port()).broadcastHandler(broadcastHandler).start();

			if (bootstrapIP != null && bootstrapPort > 0) {
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

	public void addTask(String jobID, Task task) {
		try {
			Number160 taskHash = Number160.createHash(jobID + "_" + task.id());
			FuturePut put = this.connectionPeer.put(taskHash).data(new Data(task)).start();
			put.addListener(newTaskListener(this.connectionPeer, taskHash));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private BaseFutureListener<FuturePut> newTaskListener(final PeerDHT peer, final Number160 taskHash) {
		return new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {

					NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
					dataMap.put(new Number640(taskHash, taskHash, taskHash, taskHash), new Data(taskHash));
					peer.peer().broadcast(taskHash).dataMap(dataMap).start();

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

}
