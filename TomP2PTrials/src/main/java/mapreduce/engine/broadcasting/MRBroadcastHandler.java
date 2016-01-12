package mapreduce.engine.broadcasting;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.messageconsumer.IMessageConsumer;
import mapreduce.engine.priorityexecutor.PriorityExecutor;
import mapreduce.execution.job.Job;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MRBroadcastHandler extends StructuredBroadcastHandler {

	private static Logger logger = LoggerFactory.getLogger(MRBroadcastHandler.class);

	private String executor;
	private IDHTConnectionProvider dhtConnectionProvider;
	private IMessageConsumer messageConsumer;
	private PriorityExecutor taskExecutionServer;

	private ListMultimap<Job, Future<?>> jobFuturesFor = SyncedCollectionProvider.syncedListMultimap();

	@Override
	public StructuredBroadcastHandler receive(Message message) {

		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				IBCMessage bcMessage = (IBCMessage) dataMap.get(nr).object();
				addExternallyReceivedMessage(bcMessage);
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return super.receive(message);
	}

	protected void addExternallyReceivedMessage(IBCMessage bcMessage) {
		String jobId = bcMessage.inputDomain().jobId();
		Job job = null;
		if ((job = getJob(jobId)) == null) {
			dhtConnectionProvider.get(DomainProvider.JOB, jobId).addListener(new BaseFutureAdapter<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {
					if (future.isSuccess()) {
						if (future.data() != null) {
							Job job = (Job) future.data().object();
							submit(bcMessage, job);
						}
					} else {
						logger.info("No success retrieving Job (" + jobId + ") from DHT. Try again");
					}
				}
			});
		} else {
			if (executor != null && !bcMessage.outputDomain().executor().equals(executor)) { // Don't receive it if I sent it to myself
				submit(bcMessage, job);
			}
		}

	}

	public Job getJob(String jobId) {
		for (Job job : jobFuturesFor.keySet()) {
			if (job.id().equals(jobId)) {
				return job;
			}
		}
		return null;
	}

	public void submit(IBCMessage bcMessage, Job job) {
		if (!job.isFinished()) {
			jobFuturesFor.put(job, taskExecutionServer.submit(new Runnable() {

				@Override
				public void run() {
					bcMessage.execute(job, messageConsumer);
				}
			}, job.priorityLevel(), job.creationTime(), bcMessage.procedureIndex(), bcMessage.status(), bcMessage.creationTime()));
		} else {
			List<Future<?>> jobFutures = jobFuturesFor.get(job);
			for (Future<?> jobFuture : jobFutures) {
				if (!jobFuture.isCancelled()) {
					jobFuture.cancel(true);
				}
			}
			jobFuturesFor.get(job).clear();
		}
	}

	// Setter, Getter, Creator, Constructor follow below..
	private MRBroadcastHandler(int nrOfConcurrentlyExecutedBCMessages) {
		this.taskExecutionServer = PriorityExecutor.newFixedThreadPool(nrOfConcurrentlyExecutedBCMessages);
	}

	/**
	 *
	 * 
	 * @param nrOfConcurrentlyExecutedBCMessages
	 *            number of threads for this thread pool: how many bc messages may be executed at the same time?
	 * @return
	 */
	public static MRBroadcastHandler create(int nrOfConcurrentlyExecutedBCMessages) {
		return new MRBroadcastHandler(nrOfConcurrentlyExecutedBCMessages);
	}

	public MRBroadcastHandler messageConsumer(IMessageConsumer messageConsumer) {
		this.messageConsumer = messageConsumer;
		return this;
	}

	public MRBroadcastHandler dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public MRBroadcastHandler executor(String executor) {
		this.executor = executor;
		return this;
	}

	public ListMultimap<Job, Future<?>> jobFutures() {
		return this.jobFuturesFor;
	}
}
