package firstdesignidea.execution.jobtask;

import firstdesignidea.storage.DHTConnectionProvider;

public interface IJobManager {
	public IJobManager dhtConnectionProvider(DHTConnectionProvider dhtConnectionProvider);

	public DHTConnectionProvider dhtConnectionProvider();
	// public void scheduleJobs(Number160 jobQueueLocation);
	//
	//
	// public void scheduleReduceTasks();
	//
	// public void startReducing();
	//
	// public void startMapping(Number160 jobLocation);

 
}
