package mapreduce.engine.messageconsumer;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.engine.broadcasting.BCMessageStatus;
import mapreduce.engine.broadcasting.IBCMessage;
import mapreduce.engine.priorityexecutor.ComparableBCMessageTask;
import mapreduce.engine.priorityexecutor.PriorityExecutor;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task;
import mapreduce.utils.SyncedCollectionProvider;

/**
 * <code>MessageConsumer</code> stores incoming <code>IBCMessage</code> on a queue for future processing
 * 
 * @author ozihler
 *
 */
public abstract class AbstractMessageConsumer implements IMessageConsumer {

	protected static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

//	protected SortedMap<Job, PriorityBlockingQueue<IBCMessage>> jobs;

	// private boolean canTake;

//	private int maxNrOfExecutions = 4;

	private Map<Procedure, ListMultimap<BCMessageStatus, Future<?>>> futures;

	protected AbstractMessageConsumer() {
//		this.jobs = SyncedCollectionProvider.syncedTreeMap();
		futures = SyncedCollectionProvider.syncedHashMap();
	}

 
//
//	public PriorityBlockingQueue<IBCMessage> queueFor(Job job) {
//		return jobs.get(job);
//	}
//
//	public SortedMap<Job, PriorityBlockingQueue<IBCMessage>> jobs() {
//		return jobs;
//	}

//	public AbstractMessageConsumer maxNrOfExecutions(int maxNrOfExecutions) {
//		this.maxNrOfExecutions = maxNrOfExecutions;
//		return this;
//	}

//	public boolean canExecute() {
//		logger.info("this.taskExecutionServer.getActiveCount() + this.taskExecutionServer.getQueue().size() < this.maxNrOfExecutions? "
//				+ (this.taskExecutionServer.getActiveCount() + this.taskExecutionServer.getQueue().size()) + " < " + this.maxNrOfExecutions);
//		return (this.taskExecutionServer.getActiveCount() + this.taskExecutionServer.getQueue().size()) < this.maxNrOfExecutions;
//	}

	// @Override
	// public void run() {
	// logger.info(jobs + "");
	// while (jobs.isEmpty()) {
	// logger.info("Waiting for first job");
	// try {
	// Thread.sleep(1000);
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// }
	// }
	// while (canTake()) {
	// try {
	// Job job = jobs.firstKey();
	// if (!job.isFinished()) {
	// logger.info("Before Take message");
	// IBCMessage nextMessage = jobs.get(job).take();
	// AbstractMessageConsumer msgCnsmr = this;
	// if (canExecute()) {
	// ListMultimap<BCMessageStatus, Future<?>> listMultimap = futures.get(job);
	// if (listMultimap == null) {
	// listMultimap = SyncedCollectionProvider.syncedListMultimap();
	// futures.put(job.currentProcedure(), listMultimap);
	// }
	// listMultimap.put(nextMessage.status(), taskExecutionServer.submit(new Runnable() {
	//
	// @Override
	// public void run() {
	// nextMessage.execute(job, msgCnsmr);
	// }
	// }));
	// }
	// }
	// } catch (InterruptedException | NoSuchElementException e) {
	// logger.info("Exception caught", e);
	// }
	// }
	//
	// }

	//
	// @Override
	// public AbstractMessageConsumer canTake(boolean canTake) {
	// this.canTake = canTake;
	// return this;
	// }
	//
	// @Override
	// public boolean canTake() {
	// return this.canTake;
	// }

//	public Job getJob(String jobId) {
//		for (Job job : jobs.keySet()) {
//			if (job.id().equals(jobId)) {
//				return job;
//			}
//		}
//		return null;
//	}
}