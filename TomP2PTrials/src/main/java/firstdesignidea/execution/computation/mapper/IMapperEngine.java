package firstdesignidea.execution.computation.mapper;

import java.util.Map;

import firstdesignidea.execution.jobtask.ITask;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.MapTaskStatus;
import firstdesignidea.execution.scheduling.IMapTaskSplitter;
import net.tomp2p.peers.PeerAddress;

public interface IMapperEngine {
	public IMapperEngine mapTaskSplitter(IMapTaskSplitter mapTaskSplitter);

	public IMapperEngine mapper(IMapper<?, ?, ?, ?> mapper); 
 
	public Map<ITask, Map<PeerAddress, MapTaskStatus>> split(Job jobToSplit);

 

}
