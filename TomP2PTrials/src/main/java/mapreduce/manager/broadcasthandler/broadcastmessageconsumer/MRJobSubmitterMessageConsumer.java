package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;

public class MRJobSubmitterMessageConsumer extends AbstractMessageConsumer {
	private String jobSubmitterID;

	private MRJobSubmitterMessageConsumer(String jobSubmitterID, BlockingQueue<IBCMessage> bcMessages, CopyOnWriteArrayList<Job> jobs) {
		super(bcMessages, jobs);
		this.jobSubmitterID = jobSubmitterID;
	}

	public static MRJobSubmitterMessageConsumer newInstance(String jobSubmitterID, CopyOnWriteArrayList<Job> jobs) {
		return new MRJobSubmitterMessageConsumer(jobSubmitterID, new PriorityBlockingQueue<IBCMessage>(), jobs);
	}

	@Override
	public void handleFinishedJob(Job job, String jobSubmitterId) {
		if (this.jobSubmitterID.equals(jobSubmitterId)) {
			System.err.println("Finished job " + job.id());
		}
	}

	@Override
	public MRJobSubmitterMessageConsumer canTake(boolean canTake) {
		return (MRJobSubmitterMessageConsumer) super.canTake(canTake);
	}

	 

}