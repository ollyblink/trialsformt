package firstdesignidea;

import firstdesignidea.execution.broadcasthandler.MRBroadcastHandler;
import firstdesignidea.execution.computation.combiner.DefaultCombinerEngine;
import firstdesignidea.execution.computation.combiner.ICombinerEngine;
import firstdesignidea.execution.computation.mapper.DefaultMapperEngine;
import firstdesignidea.execution.computation.mapper.IMapperEngine;
import firstdesignidea.execution.computation.reducer.DefaultReducerEngine;
import firstdesignidea.execution.computation.reducer.IReducerEngine;
import firstdesignidea.execution.datadistribution.DefaultReduceDataDistributor;
import firstdesignidea.execution.datadistribution.IReduceDataDistributor;
import firstdesignidea.execution.scheduling.DefaultMapTaskSplitter;
import firstdesignidea.execution.scheduling.IMapTaskSplitter;
import firstdesignidea.server.MRJobExecutor;
import firstdesignidea.storage.DefaultDHTConnection;
import firstdesignidea.storage.IDHTConnection;
import net.tomp2p.p2p.BroadcastHandler;

public class DesignIdeaJobExecutor {
	public static void main(String[] args) {
		String ip = "192.36.54.85";
		int port = 5000; 
		
		

		MRBroadcastHandler broadcastHandler = new MRBroadcastHandler();
		
		IDHTConnection dhtConnection = DefaultDHTConnection.newDHTConnection()
				.broadcastHandler(broadcastHandler);
		
		
		IMapperEngine mapperEngine = DefaultMapperEngine
										.newDefaultMapperEngine()
										.mapTaskSplitter(new DefaultMapTaskSplitter());
		
		IReducerEngine reducerEngine = DefaultReducerEngine
										.newDefaultReducerEngine()
										.reduceDataDistributor(new DefaultReduceDataDistributor());
 

		MRJobExecutor jobExecutor = MRJobExecutor
										.newJobExecutor()
										.ip(ip)
										.port(port)
										.dhtConnection(dhtConnection)
										.mapperEngine(mapperEngine)
										.reducerEngine(reducerEngine);
		 

		jobExecutor.startListeningAndExecuting();
	}
}
