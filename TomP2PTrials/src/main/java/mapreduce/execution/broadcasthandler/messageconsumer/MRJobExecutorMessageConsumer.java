package mapreduce.execution.broadcasthandler.messageconsumer;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.server.MRJobExecutor;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutorMessageConsumer extends AbstractMessageConsumer {

	private MRJobExecutor jobExecutor;

	private MRJobExecutorMessageConsumer(BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		super(bcMessages, jobs);
	}

	public static MRJobExecutorMessageConsumer newMRJobExecutorMessageConsumer(BlockingQueue<Job> jobs) {
		return new MRJobExecutorMessageConsumer(new PriorityBlockingQueue<IBCMessage>(), jobs);
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
		logger.warn("Updating task " + taskId + ", " + peerAddress.inetAddress() + ":" + peerAddress.tcpPort() + ", " + currentStatus + ", number of jobs: "
				+ jobs.size());
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
