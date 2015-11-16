package firstdesignidea.execution.computation.mapper;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.jobtask.ITask;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.MapTaskStatus;
import firstdesignidea.execution.scheduling.IMapTaskSplitter;
import net.tomp2p.peers.PeerAddress;

public class DefaultMapperEngine implements IMapperEngine {
	private static Logger logger = LoggerFactory.getLogger(DefaultMapperEngine.class);

	private IMapTaskSplitter mapTaskSplitter;
	private IMapper<?, ?, ?, ?> mapper; 

	private DefaultMapperEngine() {

	}

	public static DefaultMapperEngine newDefaultMapperEngine() {
		return new DefaultMapperEngine();
	}

	@Override
	public IMapperEngine mapTaskSplitter(IMapTaskSplitter mapTaskSplitter) {
		this.mapTaskSplitter = mapTaskSplitter;
		return this;
	}

	@Override
	public IMapperEngine mapper(IMapper<?, ?, ?, ?> mapper) {
		this.mapper = mapper;
		return this;
	}

	@Override
	public Map<ITask, Map<PeerAddress, MapTaskStatus>> split(Job jobToSplit) {
		return this.mapTaskSplitter.split(jobToSplit);
	}

 
 

	
 

}
