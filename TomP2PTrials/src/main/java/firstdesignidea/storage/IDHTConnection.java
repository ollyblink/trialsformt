package firstdesignidea.storage;

import firstdesignidea.execution.broadcasthandler.MRBroadcastHandler;
import firstdesignidea.execution.jobtask.JobStatus;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number160;

public interface IDHTConnection {
	/**
	 * Put some data into the dht and register a listener that will react when the method returns.
	 * 
	 * @param key DHT key
	 * @param value object to put into the DHT
	 * @param futurePutListener listener that reacts when the method returns (callback)
	 */
	public void put(Number160 key, Object value, BaseFutureListener<FuturePut> futurePutListener);

	public void add(Number160 key, Object value, BaseFutureListener<FuturePut> futurePutListener);

	public Object get(Number160 key, BaseFutureListener<FutureGet> futureGetListener);

	public void broadcast(Number160 key, JobStatus jobStatus);

	public IDHTConnection broadcastHandler(MRBroadcastHandler broadcastHandler);

	public MRBroadcastHandler broadcastHandler(); 

}
