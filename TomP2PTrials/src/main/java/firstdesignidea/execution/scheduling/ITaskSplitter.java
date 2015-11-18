package firstdesignidea.execution.scheduling;

import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.storage.DHTConnectionProvider;

public interface ITaskSplitter {

	public void splitAndEmit(final Job job, DHTConnectionProvider dhtConnectionProvider);

}
