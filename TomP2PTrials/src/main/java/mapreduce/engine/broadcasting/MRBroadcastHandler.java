package mapreduce.engine.broadcasting;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
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
	private SortedMap<Job, PriorityBlockingQueue<IBCMessage>> jobQueues;
	private IDHTConnectionProvider dhtConnectionProvider;

	private MRBroadcastHandler() {
		logger.info("Created broadcasthandler");
	}

	public static MRBroadcastHandler create() {
		return new MRBroadcastHandler();
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {

		logger.info(owner + " received message");
		if (owner == null) {
			logger.info("Owner not set! call owner(String owner)");
		}
		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				IBCMessage bcMessage = (IBCMessage) dataMap.get(nr).object();
				logger.info("message is " + bcMessage);
				addBCMessage(bcMessage);
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return super.receive(message);
	}

	private void addBCMessage(IBCMessage bcMessage) {

		Job job = getJob(bcMessage.inputDomain().jobId());
		logger.info("Job is: " + job);
		if (job == null) {
			dhtConnectionProvider.get(DomainProvider.JOB, bcMessage.inputDomain().jobId()).addListener(new BaseFutureAdapter<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {
					logger.info("Waiting");
					if (future.isSuccess()) {
						if (future.data() != null) {
							Job job = (Job) future.data().object();
							jobQueues.put(job, new PriorityBlockingQueue<>());
							jobQueues.get(job).add(bcMessage);
							logger.info("Successfully retrieved job (" + job.id() + ") from DHT .");
						}
					} else {
						logger.info("No success retrieving Job (" + bcMessage.inputDomain().jobId() + ") from DHT. Try again");
					}
				}
			});
		} else {
			logger.info("Before owner check");

			if (owner != null && !bcMessage.outputDomain().executor().equals(owner)) { // Don't receive it if I sent it to myself
				logger.info("Added message "+bcMessage+" for job "+ job + " to queue.");
				jobQueues.get(job).add(bcMessage);
			}
		}

	}

	private Job getJob(String jobId) {
		for (Job job : jobQueues.keySet()) {
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

	public MRBroadcastHandler jobQueues(SortedMap<Job, PriorityBlockingQueue<IBCMessage>> jobQueues) {
		this.jobQueues = jobQueues;
		return this;
	}

	public MRBroadcastHandler owner(String owner) {
		logger.info("Added owner: " + owner);
		this.owner = owner;
		return this;
	}

	public SortedMap<Job, PriorityBlockingQueue<IBCMessage>> jobQueues() {
		return this.jobQueues;
	}

}
