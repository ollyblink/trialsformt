package mapreduce.manager.broadcasting.broadcastmessages.jobmessages;

import mapreduce.manager.broadcasting.broadcastmessages.IBCMessage;

public interface IJobBCMessage extends IBCMessage {

	public String jobId();
}
