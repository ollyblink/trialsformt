package firstdesignidea.execution.jobtask;

import firstdesignidea.storage.DHTConnectionProvider;

public interface IJobReceiver {

	public void addJob(String taskId, String jobId);

}
