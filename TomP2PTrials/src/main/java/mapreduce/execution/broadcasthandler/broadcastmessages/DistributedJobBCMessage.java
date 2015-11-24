package mapreduce.execution.broadcasthandler.broadcastmessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.MessageConsumer;
import mapreduce.execution.jobtask.Job;
import mapreduce.server.MRJobExecutor;
import net.tomp2p.peers.PeerAddress;

public class DistributedJobBCMessage extends AbstractBCMessage{
	private static Logger logger = LoggerFactory.getLogger(DistributedJobBCMessage.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 2792435155108555907L;
	private Job job;

	private DistributedJobBCMessage() {
	}

	public static DistributedJobBCMessage newDistributedTaskBCMessage() {
		return new DistributedJobBCMessage();
	}

	@Override
	public JobStatus status() {
		return JobStatus.DISTRIBUTED_JOB;
	}

	public Job job() {
		return this.job;
	}

	public DistributedJobBCMessage job(Job job) {
		this.job = job;
		return this;
	}

	@Override
	public void execute(MessageConsumer messageConsumer) {
		logger.warn("DistributedJobBCMessage::execute()::added job");
		messageConsumer.addJob(job);

	}
	

	@Override
	public DistributedJobBCMessage sender(PeerAddress peerAddress) {
		return (DistributedJobBCMessage)super.sender(peerAddress); 
	}
}