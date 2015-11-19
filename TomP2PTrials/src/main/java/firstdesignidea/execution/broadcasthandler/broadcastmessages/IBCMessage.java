package firstdesignidea.execution.broadcasthandler.broadcastmessages;

import java.io.Serializable;

import firstdesignidea.execution.jobtask.JobStatus;
import firstdesignidea.server.MRJobExecutor;
import firstdesignidea.storage.DHTConnectionProvider;

public interface IBCMessage extends Serializable {
	public JobStatus status();

	public void execute(MRJobExecutor mrJobExecutor);

//	public void execute(DHTConnectionProvider dhtConnectionProvider);
}
