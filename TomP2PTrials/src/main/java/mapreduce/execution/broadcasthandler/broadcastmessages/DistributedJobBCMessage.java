package mapreduce.execution.broadcasthandler.broadcastmessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.messageconsumer.IMessageConsumer;
import mapreduce.execution.jobtask.Job;
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

	public static DistributedJobBCMessage newInstance() {
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
	public void execute(IMessageConsumer messageConsumer) {
		logger.warn("DistributedJobBCMessage::execute()::received job from "+sender().inetAddress()+":"+sender().tcpPort()+", added job");
		messageConsumer.addJob(job);

	}
	

	@Override
	public DistributedJobBCMessage sender(PeerAddress peerAddress) {
		return (DistributedJobBCMessage)super.sender(peerAddress); 
	}

	@Override
	public String toString() {
		return "DistributedJobBCMessage [job=" + job + ", sender=" + sender + "]";
	}
	
	
}
