package firstdesignidea;

import firstdesignidea.execution.broadcasthandler.MRBroadcastHandler;
import firstdesignidea.storage.DefaultDHTConnection;
import firstdesignidea.storage.IDHTConnection;

public class DesignIdeaJobExecutor {
	public static void main(String[] args) {
		String ip = "192.36.54.85";
		int port = 5000; 
		
		

		MRBroadcastHandler broadcastHandler = new MRBroadcastHandler();
		
		IDHTConnection dhtConnection = DefaultDHTConnection.newDHTConnection()
				.broadcastHandler(broadcastHandler);
		
		
//		IMapperEngine mapperEngine = DefaultMapperEngine
//										.newDefaultMapperEngine()
//										.mapTaskSplitter(new DefaultTaskSplitter());
//		
//		IReducerEngine reducerEngine = DefaultReducerEngine
//										.newDefaultReducerEngine()
//										.reduceDataDistributor(new DefaultReduceDataDistributor());
 
//
//		MRJobExecutor jobExecutor = MRJobExecutor
//										.newJobExecutor()
//										.ip(ip)
//										.port(port)
//										.dhtConnection(dhtConnection)
//										.mapperEngine(mapperEngine)
//										.reducerEngine(reducerEngine);
		 

//		jobExecutor.startListeningAndExecuting();
	}
}
