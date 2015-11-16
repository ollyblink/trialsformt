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
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import utils.GetOwnIpAddressTest;

public class TrialInternetConnection {
	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		final String key = "Hello World";
		Random random = new Random();
		String ipSuperPeer = "192.168.43.234";
		int port = 4000;
		int peerID = 2;
		GetOwnIpAddressTest.main(null);
		Bindings b = new Bindings();
		b.addInterface("wlan0");
		Random RND = new Random();
		if (peerID == 1) {
 			new PeerBuilder(Number160.createHash("super peer")).bindings(b).ports(port).start();

		} else {
			int port2 = port + RND.nextInt(1000) + 1;
			System.out.println("Port 2: " +port2);
			Peer myPeer = new PeerBuilder(Number160.createHash("client peer")).broadcastHandler(new MyBroadcastHandler()).bindings(b)
					.behindFirewall(true).ports(port2).enableMaintenance(false).start();
			PeerAddress bootstrapServerPeerAddress = new PeerAddress(Number160.ZERO, new InetSocketAddress(InetAddress.getByName(ipSuperPeer), port));

			FutureDiscover discovery = myPeer.discover().peerAddress(bootstrapServerPeerAddress).start();
			discovery.awaitUninterruptibly();
			if (!discovery.isSuccess()) {
				System.err.println("no success!");
			}
			System.err.println("Peer: " + discovery.reporter() + " told us about our address.");
			InetSocketAddress myInetSocketAddress = new InetSocketAddress(myPeer.peerAddress().inetAddress(), port2);
 
			bootstrapServerPeerAddress = discovery.reporter();
			FutureBootstrap bootstrap = myPeer.bootstrap().peerAddress(bootstrapServerPeerAddress).start();
			bootstrap.awaitUninterruptibly();

			if (!bootstrap.isSuccess()) {
				System.err.println("no success!");
			}
//
//			Number160 hash = Number160.createHash("Hello");
//			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
//			dataMap.put(new Number640(hash, hash, hash, hash), new Data("Broadcast hello world"));
//			myPeer.broadcast(hash).dataMap(dataMap).start();
			 PeerDHT myPeerDHT = new PeerBuilderDHT(myPeer).start();
			
			 FuturePut putFuture = myPeerDHT.put(Number160.createHash("key")).data(new Data(myInetSocketAddress)).start();
			 putFuture.awaitUninterruptibly();
			 FutureGet futureDHT = myPeerDHT.get(Number160.createHash("key")).start();
			 futureDHT.awaitUninterruptibly();
			 futureDHT.futureRequests().awaitUninterruptibly();
			 Data data = futureDHT.data();
			 if (data == null) {
			 throw new RuntimeException("Address not available in DHT.");
			 }
			 InetSocketAddress inetSocketAddress = (InetSocketAddress) data.object();
			 System.err.println("returned " + inetSocketAddress);
//			// myPeer.shutdown();
		}
	}
}
