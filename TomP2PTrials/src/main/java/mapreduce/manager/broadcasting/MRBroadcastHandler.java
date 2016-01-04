package mapreduce.manager.broadcasting;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasting.broadcastmessages.IBCMessage;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MRBroadcastHandler extends StructuredBroadcastHandler {
	protected static final int MAX_NR_OF_TRIALS = 3;

	private static Logger logger = LoggerFactory.getLogger(MRBroadcastHandler.class);

	private String owner;
	private TreeMap<Job, PriorityBlockingQueue<IBCMessage>> jobs; 
	private IDHTConnectionProvider dhtConnectionProvider;

	private MRBroadcastHandler() {

	}

	public static MRBroadcastHandler create() {
		return new MRBroadcastHandler();
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		super.receive(message);
		if (owner == null) {
			logger.info("Owner not set! call owner(String owner)");
		}
		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				IBCMessage bcMessage = (IBCMessage) dataMap.get(nr).object();
				Job job = getJob(bcMessage.inputDomain().jobId());
				if (job != null) {
					if (owner != null && !bcMessage.outputDomain().executor().equals(owner)) {
						jobs.get(job).add(bcMessage);
					}
				} else {
					tryGet(bcMessage.inputDomain().jobId(), bcMessage);
				}
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return this;
	}

	private class GetJobListener extends BaseFutureAdapter<FutureGet> {

		private IBCMessage bcMessage;
		private String jobId;
		private int putTrialCounter;

		private GetJobListener(String jobId, IBCMessage bcMessage, int putTrialCounter) {
			this.bcMessage = bcMessage;
			this.jobId = jobId;
			this.putTrialCounter = putTrialCounter;
		}

		@Override
		public void operationComplete(FutureGet future) throws Exception {
			if (future.isSuccess()) {
				if (future.data() != null) {
					Job job = (Job) future.data().object();
					jobs.put(job, new PriorityBlockingQueue<>());
					jobs.get(job).add(bcMessage);
					logger.info("Successfully retrieved job (" + jobId + ") from DHT, trial nr: " + putTrialCounter + ".");
				}
			} else {
				if (putTrialCounter++ < MAX_NR_OF_TRIALS) {
					logger.info("Trial nr " + (putTrialCounter - 1) + ": No success retrieving Job (" + jobId + ") from DHT. Try again"); 
					dhtConnectionProvider.get(DomainProvider.JOB, jobId).addListener(new GetJobListener(jobId, bcMessage, putTrialCounter));
				} else {
					logger.info("FINAL TRIAL nr " + (putTrialCounter - 1) + ": No success retrieving Job (" + jobId + ") from DHT.");
				}
			}
		}

	}

	private void tryGet(String jobId, IBCMessage bcMessage) {
		int putTrialCounter = 0;
		dhtConnectionProvider.get(DomainProvider.JOB, jobId).addListener(new GetJobListener(jobId, bcMessage, putTrialCounter));
	}

	private Job getJob(String jobId) {
		for (Job job : jobs.keySet()) {
			if (job.id().equals(jobId)) {
				return job;
			}
		}
		return null;
	}

	public MRBroadcastHandler dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public MRBroadcastHandler jobQueues(TreeMap<Job, PriorityBlockingQueue<IBCMessage>> jobs) {
		this.jobs = jobs;
		return this;
	}

	public MRBroadcastHandler owner(String owner) {
		this.owner = owner;
		return this;
	}

}
