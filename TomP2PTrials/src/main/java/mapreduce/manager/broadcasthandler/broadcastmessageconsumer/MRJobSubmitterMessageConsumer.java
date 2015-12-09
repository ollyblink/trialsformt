package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class MRJobSubmitterMessageConsumer extends AbstractMessageConsumer {
	private String jobSubmitterID;

	private MRJobSubmitterMessageConsumer(String jobSubmitterID, BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		super(bcMessages, jobs);
		this.jobSubmitterID = jobSubmitterID;
	}

	public static MRJobSubmitterMessageConsumer newInstance(String jobSubmitterID, BlockingQueue<Job> jobs) {
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
