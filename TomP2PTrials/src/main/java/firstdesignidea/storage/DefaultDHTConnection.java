package firstdesignidea.storage;

import java.io.IOException;
import java.util.TreeMap;

import firstdesignidea.execution.broadcasthandler.MRBroadcastHandler;
import firstdesignidea.execution.computation.mapper.IMapperEngine;
import firstdesignidea.execution.computation.reducer.IReducerEngine;
import firstdesignidea.execution.jobtask.JobStatus;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class DefaultDHTConnection implements IDHTConnection {

	/** Actual connection to the DHT */
	private PeerDHT peer;
	/** Listens to all incoming broadcast calls and acts upon them */
	private MRBroadcastHandler broadcastHandler;

	/**
	 * Constructor function for Fluent API. Don't try to use a constructor!
	 * 
	 * @return a new IDHTConnection object
	 */
	public static IDHTConnection newDHTConnection() {
		return new DefaultDHTConnection();
	}

	private DefaultDHTConnection() {

	}

	@Override
	public IDHTConnection broadcastHandler(MRBroadcastHandler broadcastHandler) {
		this.broadcastHandler = broadcastHandler;
		return this;
	}

	@Override
	public void broadcast(Number160 key, JobStatus jobStatus) {

		try {
			Data data = new Data(jobStatus);
			TreeMap<Number640, Data> treeMap = new TreeMap<Number640, Data>(); 
			treeMap.put(new Number640(key, key, key, key), data); //TODO: how to use this? 
			peer.peer().broadcast(key).dataMap(treeMap).start();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void put(Number160 key, Object value, BaseFutureListener<FuturePut> futurePutListener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void add(Number160 key, Object value, BaseFutureListener<FuturePut> futurePutListener) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object get(Number160 key, BaseFutureListener<FutureGet> futureGetListener) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MRBroadcastHandler broadcastHandler() {
		return this.broadcastHandler;
	}

}
