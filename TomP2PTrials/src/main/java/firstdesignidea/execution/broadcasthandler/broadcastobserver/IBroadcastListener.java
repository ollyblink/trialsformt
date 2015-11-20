package firstdesignidea.execution.broadcasthandler.broadcastobserver;

import firstdesignidea.execution.broadcasthandler.broadcastmessages.IBCMessage;

public interface IBroadcastListener {

//	public IBroadcastListener broadcastDistributor(IBroadcastDistributor connectionProvider);

	public void inform(IBCMessage bcMessage);
}
