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
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MRBroadcastHandler extends StructuredBroadcastHandler {
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

		if (owner == null) {
			logger.info("Owner not set! call owner(String owner)");
		}
		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				IBCMessage bcMessage = (IBCMessage) dataMap.get(nr).object();
				Job job = getJob(bcMessage.jobId());
				if (job != null) {
					if (owner != null && !bcMessage.sender().equals(owner)) {
						jobs.get(job).add(bcMessage);
					}
				} else {
					tryGet(job, bcMessage.jobId());
				}
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return super.receive(message);
	}

	private void tryGet(Job job, String jobId) {
		this.dhtConnectionProvider.getAll(DomainProvider.INSTANCE, jobId);
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

	public MRBroadcastHandler jobs(TreeMap<Job, PriorityBlockingQueue<IBCMessage>> jobs) {
		this.jobs = jobs;
		return this;
	}

	public MRBroadcastHandler owner(String owner) {
		this.owner = owner;
		return this;
	}

}
