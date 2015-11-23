package mapreduce.execution.broadcasthandler.broadcastmessages;

import java.io.Serializable;

import mapreduce.execution.jobtask.JobStatus;
import mapreduce.server.MRJobExecutor;

public interface IBCMessage extends Serializable, Comparable<IBCMessage> {
	public JobStatus status();

	public void execute(MessageConsumer messageConsumer);

}
