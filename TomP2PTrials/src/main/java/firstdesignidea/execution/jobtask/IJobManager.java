package firstdesignidea.execution.jobtask;

import net.tomp2p.peers.Number160;

public interface IJobManager {

	public void scheduleJobs(Number160 jobQueueLocation);

 
	public void scheduleReduceTasks();

	public void startReducing();

	public void startMapping(Number160 jobLocation);

}
