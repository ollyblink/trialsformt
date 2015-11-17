package firstdesignidea.client;

import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.exceptions.IncorrectFormatException;
import firstdesignidea.execution.exceptions.NotSetException;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.scheduling.ITaskSplitter;
import firstdesignidea.utils.FormatUtils;
import firstdesignidea.utils.PortGenerator;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class MRJobSubmitter {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmitter.class);
	private static final int MIN_PORT = 4000;
	static final Random RND = new Random(42L);
	private String ip;
	private int port;
	private PeerDHT connectionPeer;
	private ITaskSplitter taskSplitter;

	private MRJobSubmitter() {
	}

	public static MRJobSubmitter newMapReduceJobSubmitter() {
		return new MRJobSubmitter();
	}

	public MRJobSubmitter ip(String ip) {
		this.ip = ip;
		return this;
	}

	public MRJobSubmitter port(int port) {
		this.port = port;
		return this;
	}

	public String ip() throws IncorrectFormatException, NotSetException {
		if (ip != null) {
			if (FormatUtils.isCorrectIP4(ip)) {
				return this.ip;
			} else {
				throw new IncorrectFormatException("This IP has not the correct format.");
			}
		} else {
			throw new NotSetException("Could not find a valid IP.");
		}
	}

	public int port() throws IncorrectFormatException, NotSetException {
		if (port <= MIN_PORT) {
			this.port = PortGenerator.generatePort();
		}
		return this.port;
	}

	public void disconnect() {
		// TODO Auto-generated method stub

	}

	/**
	 * 
	 * @param job
	 * @return
	 */
	public Object submit(Job job) {
		try {
			if (this.connectionPeer == null) {
				this.connectionPeer = new PeerBuilderDHT(new PeerBuilder(new Number160(RND)).ports(port).start()).start();
			}

			// TODO add callback for this MRJobSubmitter
			FuturePut put = this.connectionPeer.add(Number160.createHash("jobqueue")).data(new Data(job)).start();
			put.addListener(putNewJobListener(this.connectionPeer));

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private BaseFutureListener<FuturePut> putNewJobListener(final PeerDHT peer) {
		return new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {

					// Broadcast that a new job was put into the DHT
					BroadcastBuilder broadcast = peer.peer().broadcast(Number160.createHash("new job submission"));
					broadcast.start();
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
