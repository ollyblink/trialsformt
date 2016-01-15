package mapreduce.engine.broadcasting.broadcasthandlers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.multithreading.PriorityExecutor;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.Procedures;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MapReduceBroadcastHandler extends StructuredBroadcastHandler {

	private static Logger logger = LoggerFactory.getLogger(MapReduceBroadcastHandler.class);

	private IDHTConnectionProvider dhtConnectionProvider;
	private IMessageConsumer messageConsumer;
	private PriorityExecutor taskExecutionServer;

	private ListMultimap<Job, Future<?>> jobFuturesFor = SyncedCollectionProvider.syncedArrayListMultimap();

	private Map<Job, Timeout> timeouts = SyncedCollectionProvider.syncedHashMap();

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
							for(Procedure procedure: job.procedures()){
								if(procedure.executable() instanceof String){//Means a java script function --> convert
									procedure.executable(Procedures.convertJavascriptToJava((String) procedure.executable()));
								}
							}
							submit(bcMessage, job);
						}
					} else {
						logger.info("No success retrieving Job (" + jobId + ") from DHT. Try again");
					}
				}
			});
		} else {// Don't receive it if I sent it to myself
			if (messageConsumer.executor().id() != null && !bcMessage.outputDomain().executor().equals(messageConsumer.executor().id())) {
				submit(bcMessage, job);
			}
		}

	}

	public void submit(IBCMessage bcMessage, Job job) {
		logger.info("Submitted job: " + job);
		if (!job.isFinished()) {

			jobFuturesFor.put(job, taskExecutionServer.submit(new Runnable() {

				@Override
				public void run() {
					bcMessage.execute(job, messageConsumer);
				}
			}, job.priorityLevel(), job.creationTime(), bcMessage.procedureIndex(), bcMessage.status(), bcMessage.creationTime()));
			updateTimestamp(job, bcMessage);
		} else {
			synchronized (this) {
				abortJobExecution(job);
			}
		}
	}

	private void abortJobExecution(Job job) {
		List<Future<?>> jobFutures = jobFuturesFor.get(job);
		for (Future<?> jobFuture : jobFutures) {
			if (!jobFuture.isCancelled()) {
				jobFuture.cancel(true);
			}
		}
	}

	private class Timeout implements Runnable {
		private MapReduceBroadcastHandler broadcastHandler;
		private Job job;
		private volatile long currentTimestamp;
		private long timeToLive;
		private long timeToSleep = 5000;
		private IBCMessage bcMessage;

		private Timeout(MapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp) {
			this.broadcastHandler = broadcastHandler;
			this.job = job;
			this.currentTimestamp = currentTimestamp;
			this.timeToLive = job.timeToLive();
		}

		private Timeout currentTimestamp(long currentTimestamp, IBCMessage bcMessage) {
			logger.info("Timeout: updated timeout for job " + job);
			this.currentTimestamp = currentTimestamp;
			this.bcMessage = bcMessage;
			return this;
		}

		@Override
		public void run() {
			while ((System.currentTimeMillis() - currentTimestamp) < timeToLive) {
				try {
					logger.info("Timeout: sleeping for " + timeToSleep);
					Thread.sleep(timeToSleep);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			synchronized (this.broadcastHandler) {
				logger.info("Timeout: aborted job " + job);
				if (bcMessage.inputDomain().procedureIndex() == -1
						&& (broadcastHandler.messageConsumer.executor() instanceof JobCalculationExecutor)) {
					// handle start differently first, because it may be due to expected file size that is not the same...
					JobCalculationExecutor calcExecutor = (JobCalculationExecutor) broadcastHandler.messageConsumer.executor();
					int actualTasksSize = job.currentProcedure().tasks().size();
					int expectedTasksSize = bcMessage.inputDomain().tasksSize();
					if (actualTasksSize < expectedTasksSize) {
						job.currentProcedure().dataInputDomain().tasksSize(actualTasksSize);
						calcExecutor.tryFinishProcedure(job.currentProcedure());
					}
				} else {
					this.broadcastHandler.abortJobExecution(job);
				}
			}
		}

	};

	private void updateTimestamp(Job job, IBCMessage bcMessage) {
		if (timeouts.containsKey(job)) {
			timeouts.get(job).currentTimestamp(System.currentTimeMillis(), bcMessage);
		} else {
			Timeout timeout = new Timeout(this, job, System.currentTimeMillis());
			timeouts.put(job, timeout);
			Thread thread = new Thread(timeout);// timeoutcounter for job
			thread.start();
		}
	}

	// Setter, Getter, Creator, Constructor follow below..
	private MapReduceBroadcastHandler(int nrOfConcurrentlyExecutedBCMessages) {
		this.taskExecutionServer = PriorityExecutor.newFixedThreadPool(nrOfConcurrentlyExecutedBCMessages);

	}

	public Job getJob(String jobId) {
		for (Job job : jobFuturesFor.keySet()) {
			if (job.id().equals(jobId)) {
				return job;
			}
		}
		return null;
	}

	/**
	 *
	 * 
	 * @param nrOfConcurrentlyExecutedBCMessages
	 *            number of threads for this thread pool: how many bc messages may be executed at the same time?
	 * @return
	 */
	public static MapReduceBroadcastHandler create(int nrOfConcurrentlyExecutedBCMessages) {
		return new MapReduceBroadcastHandler(nrOfConcurrentlyExecutedBCMessages);
	}

	public MapReduceBroadcastHandler messageConsumer(IMessageConsumer messageConsumer) {
		this.messageConsumer = messageConsumer;
		return this;
	}

	public MapReduceBroadcastHandler dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public ListMultimap<Job, Future<?>> jobFutures() {
		return this.jobFuturesFor;
	}
}
