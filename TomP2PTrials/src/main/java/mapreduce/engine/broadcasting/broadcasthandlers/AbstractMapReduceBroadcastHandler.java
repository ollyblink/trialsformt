package mapreduce.engine.broadcasting.broadcasthandlers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Future;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.broadcasthandlers.timeout.AbstractTimeout;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.multithreading.PriorityExecutor;
import mapreduce.execution.jobs.Job;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public abstract class AbstractMapReduceBroadcastHandler extends StructuredBroadcastHandler {
	// private static Logger logger = LoggerFactory.getLogger(AbstractMapReduceBroadcastHandler.class);

	protected IDHTConnectionProvider dhtConnectionProvider;
	protected IMessageConsumer messageConsumer;
	protected PriorityExecutor taskExecutionServer;

	protected ListMultimap<Job, Future<?>> jobFuturesFor = SyncedCollectionProvider.syncedArrayListMultimap();

	protected Map<Job, AbstractTimeout> timeouts = SyncedCollectionProvider.syncedHashMap();

	protected AbstractMapReduceBroadcastHandler(int nrOfConcurrentlyExecutedBCMessages) {
		this.taskExecutionServer = PriorityExecutor.newFixedThreadPool(nrOfConcurrentlyExecutedBCMessages);
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {

		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				IBCMessage bcMessage = (IBCMessage) dataMap.get(nr).object();
				evaluateReceivedMessage(bcMessage);
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return super.receive(message);
	}

	public void abortJobExecution(Job job) {
		List<Future<?>> jobFutures = jobFuturesFor.get(job);
		for (Future<?> jobFuture : jobFutures) {
			if (!jobFuture.isCancelled()) {
				jobFuture.cancel(true);
			}
		}
	}

	protected void updateTimestamp(Job job, IBCMessage bcMessage) {
		if (timeouts.containsKey(job)) {
			timeouts.get(job).currentTimestamp(System.currentTimeMillis(), bcMessage);
		} else {
			AbstractTimeout timeout = AbstractTimeout.create(this, job, System.currentTimeMillis());
			timeouts.put(job, timeout);
			Thread thread = new Thread(timeout);// timeoutcounter for job
			thread.start();
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

	public AbstractMapReduceBroadcastHandler messageConsumer(IMessageConsumer messageConsumer) {
		this.messageConsumer = messageConsumer;
		return this;
	}

	public AbstractMapReduceBroadcastHandler dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public ListMultimap<Job, Future<?>> jobFutures() {
		return this.jobFuturesFor;
	}

	public String executorId() {
		return this.messageConsumer.executor().id();
	}

	public IMessageConsumer messageConsumer() {
		return this.messageConsumer;
	}

	/**
	 * Decide on what to do with an externally received message
	 * 
	 * @param bcMessage
	 */
	protected abstract void evaluateReceivedMessage(IBCMessage bcMessage);

	/**
	 * Decide on what to do internally with the message
	 * 
	 * @param bcMessage
	 * @param job
	 */
	public abstract void processMessage(IBCMessage bcMessage, Job job);

}
