import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import utils.GetOwnIpAddressTest;

public class TrialInternetConnection {
	private static Peer master;

	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		// final String key = "Hello World";
		Random random = new Random();
		String ipSuperPeer = "192.168.43.234";
		int port = 4000;
		GetOwnIpAddressTest.main(null);
		Bindings b = new Bindings();
		b.addInterface("wlan0");

		int peerID = 2;
		String action = "";
		final String key = "C";
		final String value = "asdfasdf";
		final String bc = "Pakuu";

		Random RND = new Random();
		if (peerID == 1) {
			master = new PeerBuilder(Number160.createHash("super peer")).ports(port).broadcastHandler(new MyBroadcastHandler()).start();
			PeerDHT masterDHT = new PeerBuilderDHT(master).start();
 
		} else {
			int port2 = port + RND.nextInt(1000) + 1;
			System.out.println("Port 2: " + port2);
			final Peer myPeer = new PeerBuilder(Number160.createHash("client peer")).broadcastHandler(new MyBroadcastHandler()).behindFirewall(true)
					.ports(port2).enableMaintenance(false).start();
			PeerAddress bootstrapServerPeerAddress = new PeerAddress(Number160.ZERO, new InetSocketAddress(InetAddress.getByName(ipSuperPeer), port));

			FutureDiscover discovery = myPeer.discover().peerAddress(bootstrapServerPeerAddress).start();
			discovery.awaitUninterruptibly();
			if (!discovery.isSuccess()) {
				System.err.println("A no success!");
			}
			System.err.println("Peer: " + discovery.reporter() + " told us about our address.");
			bootstrapServerPeerAddress = discovery.reporter();
			FutureBootstrap bootstrap = myPeer.bootstrap().peerAddress(bootstrapServerPeerAddress).start();

			bootstrap.awaitUninterruptibly();

			if (!bootstrap.isSuccess()) {
				System.err.println("B no success!");
			}

			// System.out.println("Peers: " + myPeer.peerBean().peerMap());
			//
			PeerDHT myPeerDHT = new PeerBuilderDHT(myPeer).start();
			if (action.equals("")) {
				Number160 hash = Number160.createHash("Hello");
				final NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
				dataMap.put(new Number640(hash, hash, hash, hash), new Data(bc));
				myPeer.broadcast(hash).dataMap(dataMap).start();

				StructuredBroadcastHandler d = (StructuredBroadcastHandler) myPeer.broadcastRPC().broadcastHandler();
				System.out.println(d.broadcastCounter());
				while(d.broadcastCounter() <= 1){
					System.out.println("Here");
					Thread.sleep(200);
				}
			}
			//
			if (action.equals("PUT")) {

				FuturePut putFuture = myPeerDHT.add(Number160.createHash(key)).data(new Data(value)).start();
				putFuture.addListener(new BaseFutureListener<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							System.out.println("Success on put"); 
							final NavigableMap<Number640, Data> dataMap2 = new TreeMap<Number640, Data>();
							dataMap2.put(new Number640(Number160.createHash("put"), Number160.createHash("put"), Number160.createHash("put"),
									Number160.createHash("put")), new Data("put"));
							myPeer.broadcast(Number160.createHash("put")).dataMap(dataMap2).start();
						} else {
							System.out.println("C No success");
						}
					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						t.printStackTrace();
					}

				});
			}
			if (action.equals("GET")) {
				FutureGet futureDHT = myPeerDHT.get(Number160.createHash(key)).start();
				futureDHT.addListener(new BaseFutureListener<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							System.out.println(future.data().object());
						} else {
							System.out.println("C No success");
						}
					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						t.printStackTrace();
					}

				});
				// Data data = futureDHT.data();
				// if (data == null) {
				// throw new RuntimeException("Address not available in DHT.");
				// }
				// InetSocketAddress inetSocketAddress = (InetSocketAddress) data.object();
				// System.err.println("returned " + inetSocketAddress);
			}
			// myPeer.shutdown();
		}
	}
}
