package firstdesignidea.execution.scheduling;

import java.util.Map;

import firstdesignidea.execution.jobtask.ITask;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.MapTaskStatus;
import net.tomp2p.peers.PeerAddress;

public interface IMapTaskSplitter {

	public Map<ITask, Map<PeerAddress, MapTaskStatus>> split(Job job);

}
