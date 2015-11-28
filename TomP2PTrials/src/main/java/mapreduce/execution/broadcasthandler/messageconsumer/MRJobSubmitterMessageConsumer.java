package mapreduce.execution.broadcasthandler.messageconsumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.jobtask.Job;

public class MRJobSubmitterMessageConsumer extends AbstractMessageConsumer {
	private String jobSubmitterID;

	private MRJobSubmitterMessageConsumer(String jobSubmitterID, BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		super(bcMessages, jobs);
		this.jobSubmitterID = jobSubmitterID;
	}

	public static MRJobSubmitterMessageConsumer newMRJobSubmitterMessageConsumer(String jobSubmitterID, BlockingQueue<Job> jobs) {
		return new MRJobSubmitterMessageConsumer(jobSubmitterID, new PriorityBlockingQueue<IBCMessage>(), jobs);
	}

	@Override
	public void handleFinishedJob(String jobId, String jobSubmitterId) {
		if (this.jobSubmitterID.equals(jobSubmitterId)) {
			System.err.println("Finished job " + jobId);
		}
	}

	@Override
	public MRJobSubmitterMessageConsumer canTake(boolean canTake) {
		return (MRJobSubmitterMessageConsumer) super.canTake(canTake);
	}

	@Override
	protected void handleBCMessage(IBCMessage nextMessage) {
		// TODO Auto-generated method stub
		
	}

}
