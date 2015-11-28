package mapreduce.execution.broadcasthandler.messageconsumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager.AbstractJobStatusManager;
import mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager.DistributedJobJobStatusManager;
import mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager.ExecutingAndFinishedJobJobStatusManager;
import mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager.FinishedAllTasksJobStatusManager;
import mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager.FinishedTaskJobStatusManager;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.server.MRJobExecutor;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutorMessageConsumer extends AbstractMessageConsumer {

	private MRJobExecutor jobExecutor;
	private Map<JobStatus, AbstractJobStatusManager> managers;

	private MRJobExecutorMessageConsumer(BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		super(bcMessages, jobs);
		// managers = new HashMap<JobStatus, AbstractJobStatusManager>();
		// AbstractJobStatusManager manager = ExecutingAndFinishedJobJobStatusManager.newInstance();
		// managers.put(JobStatus.EXECUTING_TASK, manager);
		// managers.put(JobStatus.FINISHED_TASK, manager);
		// managers.put(JobStatus.TASK_FAILED, manager);
		// manager.start();
		// manager = DistributedJobJobStatusManager.newInstance();
		// managers.put(JobStatus.DISTRIBUTED_JOB, manager);
		// manager = FinishedAllTasksJobStatusManager.newInstance();
		// manager.start();
		// managers.put(JobStatus.FINISHED_ALL_TASKS, manager);
		// manager = FinishedTaskJobStatusManager.newInstance();
		// manager.start();
		// managers.put(JobStatus.FINISHED_JOB, manager);

	}

	public static MRJobExecutorMessageConsumer newMRJobExecutorMessageConsumer(BlockingQueue<Job> jobs) {
		return new MRJobExecutorMessageConsumer(new PriorityBlockingQueue<IBCMessage>(), jobs);
	}

	@Override
	protected void handleBCMessage(IBCMessage message) {
		message.execute(this);
	}

	/**
	 * Use this for interrupting execution (canExecute(false))
	 * 
	 * @param mrJobExecutor
	 * @return
	 */
	public MRJobExecutorMessageConsumer jobExecutor(MRJobExecutor mrJobExecutor) {
		this.jobExecutor = mrJobExecutor;
		return this;
	}

	public void addJob(Job job) {
		logger.warn("Adding new job " + job.id());
		if (!jobs.contains(job)) {
			jobs.add(job);
		}
	}

	public void updateTask(String jobId, String taskId, PeerAddress peerAddress, JobStatus currentStatus) {
		logger.warn("Updating task " + taskId + ", " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + ", " + currentStatus
				+ ", number of jobs: " + jobs.size());
		for (Job job : jobs) {
			if (job.id().equals(jobId)) {
				job.updateTaskStatus(taskId, peerAddress, currentStatus);
			}
		}
	}

	public void handleFinishedTasks(String jobId, Collection<Task> tasks) {
		for (Job job : jobs) {
			if (job.id().equals(jobId)) {
				job.synchronizeFinishedTasksStati(tasks);
			}
		}
	}

	@Override
	public MRJobExecutorMessageConsumer canTake(boolean canTake) {
		return (MRJobExecutorMessageConsumer) super.canTake(canTake);
	}

}
