package firstdesignidea.client;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.exceptions.IncorrectFormatException;
import firstdesignidea.execution.exceptions.NotSetException;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;
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
import net.tomp2p.peers.Number640;
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

	/**
	 * 
	 * @param job
	 * @return
	 */
	public Object submit(final Job job) {
		try {
			if (this.connectionPeer == null) {
				this.connectionPeer = new PeerBuilderDHT(new PeerBuilder(new Number160(RND)).ports(port).start()).start();
			}

			List<Task> tasks = taskSplitter.split(job);

			for (Task task : tasks) {
				FuturePut put = this.connectionPeer.add(Number160.createHash(task.id())).data(new Data(task)).start();
				put.addListener(putNewJobListener(this.connectionPeer, job));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private BaseFutureListener<FuturePut> putNewJobListener(final PeerDHT peer, final Job job) {
		return new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {

					NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
					Number160 hash = Number160.createHash(new Random().nextInt(Integer.MAX_VALUE));
					dataMap.put(new Number640(hash,hash,hash,hash), new Data(job));
					// Broadcast that a new job was put into the DHT
					peer.peer().broadcast(Number160.createHash("new job submission")).dataMap(dataMap).start();
					
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
