package firstdesignidea.execution.broadcasthandler.broadcastmessages;

import java.io.Serializable;

import firstdesignidea.execution.jobtask.JobStatus;

public interface IBCMessage extends Serializable {
	public JobStatus status();
}
