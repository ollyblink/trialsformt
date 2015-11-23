package mapreduce.execution.broadcasthandler.broadcastobserver;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;

public interface IBroadcastListener {

//	public IBroadcastListener broadcastDistributor(IBroadcastDistributor connectionProvider);

	public void inform(IBCMessage bcMessage);
}
