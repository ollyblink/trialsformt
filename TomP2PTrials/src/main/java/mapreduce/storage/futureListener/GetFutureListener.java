package mapreduce.storage.futureListener;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number640;

public class GetFutureListener implements BaseFutureListener<FutureGet> {
	private static Logger logger = LoggerFactory.getLogger(GetFutureListener.class);
	private String keyString;
	private Collection<Object> keyCollector;
	private String domainString;
	private boolean asList;

	private GetFutureListener() {

	}

	private GetFutureListener(String keyString, Collection<Object> keyCollector, String domainString, boolean asList) {
		this.keyString = keyString;
		this.keyCollector = keyCollector;
		this.domainString = domainString;
		this.asList = asList;
	}

	@Override
	public void operationComplete(FutureGet future) throws Exception {
		if (future.isSuccess()) {
			try {
				if (future.dataMap() != null) {
					for (Number640 n : future.dataMap().keySet()) {
						Object valueObject = null;
						if (asList) {
							valueObject = ((Value) future.dataMap().get(n).object()).value();
						} else {
							valueObject = future.dataMap().get(n).object();
						}
						synchronized (keyCollector) {
							keyCollector.add(valueObject);
						}
						logger.info(
								"getKVD: Successfully retrieved value for <K, Domain>: <" + keyString + ", " + domainString + ">: " + valueObject);
					}
				} else {
					logger.warn("getKVD: Value for <K, Domain>: <" + keyString + ", " + domainString + "> is null!");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			logger.error("getKVD: Failed trying to retrieve value for <K, Domain>: <" + keyString + ", " + domainString + ">");
		}
	}

	@Override
	public void exceptionCaught(Throwable t) throws Exception {
		logger.debug("getKVD: Exception caught", t);

	}

	public static GetFutureListener newInstance(String keyString, Collection<Object> keyCollector, String domainString, boolean asList) {
		return new GetFutureListener(keyString, keyCollector, domainString, asList);
	}
}
