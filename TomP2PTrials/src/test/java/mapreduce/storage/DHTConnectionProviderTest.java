package mapreduce.storage;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

import mapreduce.engine.broadcasting.broadcasthandlers.MapReduceBroadcastHandler;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;

public class DHTConnectionProviderTest {
	private Random random = new Random();

	@Test
	public void simplePutGetOverNetwork() throws InterruptedException {
		int bootstrapPort = random.nextInt(40000) + 4000;
		IDHTConnectionProvider dhtCon = DHTConnectionProvider.create("192.168.43.65", bootstrapPort, bootstrapPort).nrOfPeers(1)
				.broadcastHandler(MapReduceBroadcastHandler.create(1)).storageFilePath("C:\\Users\\Oliver\\Desktop\\storage");

		try {
			dhtCon.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		int other = random.nextInt(40000) + 4000;
		IDHTConnectionProvider dhtCon2 = DHTConnectionProvider.create("192.168.43.65", bootstrapPort, other).nrOfPeers(1)
				.broadcastHandler(MapReduceBroadcastHandler.create(1)).storageFilePath("C:\\Users\\Oliver\\Desktop\\storage");

		try {
			dhtCon2.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		dhtCon.put("Hello", 1, "Mydomain").addListener(new BaseFutureAdapter<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					dhtCon2.get("Hello", "Mydomain").addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								if (future.data() != null) {
									Integer number = (Integer) future.data().object();
									System.err.println("Hello: " + number);
									assertEquals(new Integer(1), number);
									dhtCon.shutdown();
									dhtCon2.shutdown();
								}
							}
						}
					}).awaitListenersUninterruptibly();
				}
			}
		}).awaitListenersUninterruptibly();
		Thread.sleep(3000);
	}

	@Test
	public void testGet() {
		
	}
}
