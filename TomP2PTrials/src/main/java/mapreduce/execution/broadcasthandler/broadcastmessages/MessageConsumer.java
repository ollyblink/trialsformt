package mapreduce.execution.broadcasthandler.broadcastmessages;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.JobStatus;
import mapreduce.execution.jobtask.Task;
import net.tomp2p.peers.PeerAddress;

/**
 * <code>MessageConsumer</code> stores incoming <code>IBCMessage</code> on a queue for future processing
 * 
 * @author ozihler
 *
 */
public class MessageConsumer implements Runnable {
	private static final long DEFAULT_SLEEPING_TIME = 100;

	private static Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

	private BlockingQueue<IBCMessage> bcMessages;

	private BlockingQueue<Job> jobs;

	private boolean canTake;

	private long sleepingTime;

	private MessageConsumer(BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		this.bcMessages = bcMessages;
		this.jobs = jobs;

	}

	public static MessageConsumer newMessageConsumer(BlockingQueue<Job> jobs) {
		return new MessageConsumer(new PriorityBlockingQueue<IBCMessage>(), jobs);
	}

	public BlockingQueue<Job> jobs() {
		return jobs;
	}

	@Override
	public void run() {
		final MessageConsumer messageConsumer = this;
		try {
			while (canTake()) {
				logger.info("MessageConsumer::run(): number of BC messages: " + bcMessages.size());
				final IBCMessage nextMessage = bcMessages.take();
				nextMessage.execute(messageConsumer);
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public MessageConsumer canTake(boolean canTake) {
		this.canTake = canTake;
		return this;
	}

	public boolean canTake() {
		return this.canTake;
	}

	public MessageConsumer sleepingTime(long sleepingTime) {
		this.sleepingTime = sleepingTime;
		return this;
	}

	public long sleepingTime() {
		return (this.sleepingTime == 0 ? DEFAULT_SLEEPING_TIME : this.sleepingTime);
	}

	public BlockingQueue<IBCMessage> queue() {
		return this.bcMessages;
	}

	public void addJob(Job job) {
		logger.warn("Adding new job " + job.id());
		if (!jobs.contains(job)) {
			jobs.add(job);
		}
	}

	public void updateTask(String jobId, String taskId, PeerAddress peerAddress, JobStatus currentStatus) {
		// logger.warn("Updating jobs");
		for (Job job : jobs) {
			if (job.id().equals(jobId)) {
				List<Task> tasks = job.tasksFor(job.nextProcedure());

				synchronized (tasks) {
					for (Task task : tasks) {

						if (task.id().equals(taskId)) {
							task.updateExecutingPeerStatus(peerAddress, currentStatus);
						}
					}
				}

			}

		}
	}

	public void createNewTasksForNextProcedure(String jobId) {

		logger.warn("NEXT JOB TO EXECUTE");
		// for (Job job : jobs) {
		// if (job.id().equals(jobId)) {
		// // IMapReduceProcedure<?, ?, ?, ?> procedure = job.procedure(job.nextProcedure().procedureNr() + 1);
		// // if (procedure == null) {
		// // this.dhtConnectionProvider.broadcastFinishedJob(job);
		// // }
		// break;
		// } else {
		// logger.warn("NEXT JOB TO EXECUTE");
		// }
		// }

	}

	public void handleFinishedJob(String jobId) {
		logger.warn("FINISHED JOB WITH JOBID:" + jobId);
	}

	public static void main(String[] args) {
		List<Integer> list = Collections.synchronizedList(new LinkedList<Integer>());

		ExecutorService s = Executors.newFixedThreadPool(2);
		for (int i = 0; i < 2; ++i) {
			s.execute(new MC(list));
		}
		s.shutdown();
		while (!s.isTerminated()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		synchronized (list) {
			for (Integer i : list) {
				System.out.println(i);
			}
		}

	}

}

class MC implements Runnable {
	private static final Random RND = new Random();
	private static int counter = 0;
	private static int idcntr = 1;
	private int id = idcntr++;
	private List<Integer> list;

	public MC(List<Integer> list) {
		this.list = list;
	}

	@Override
	public void run() {
		for (int i = 0; i < 10; ++i) {
			System.out.println("List size: " + list.size());

			synchronized (list) {
				list.add(counter++);
			}
			System.out.println(id + " added " + counter);
			try {
				Thread.sleep(RND.nextInt(1000));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}