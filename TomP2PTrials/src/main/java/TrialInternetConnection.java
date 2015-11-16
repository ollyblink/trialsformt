import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Random;

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
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import utils.GetOwnIpAddressTest;

public class TrialInternetConnection {
	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		final String key = "Hello World";
		Random random = new Random();
		String ip = "192.168.43.234";
		int port = 4000;
		int peerID = 1;
		GetOwnIpAddressTest.main(null);
		if (peerID == 1) {
			Bindings b = new Bindings();
			Random RND = new Random();
			b.addInterface("wlan0");

			new PeerBuilder(Number160.createHash("super peer")).bindings(b).behindFirewall(true).ports(port).start();

		} else {

			Bindings b = new Bindings();
			b.addInterface("wlan1");

			Peer peer = new PeerBuilder(Number160.createHash("client peer")).bindings(b).ports(port + random.nextInt(1000)).behindFirewall(true)
					.start();
			PeerAddress bootstrapServerPeerAddress = new PeerAddress(Number160.ZERO, new InetSocketAddress(InetAddress.getByName(ip), port));
			FutureDiscover discovery = peer.discover().peerAddress(bootstrapServerPeerAddress).start();
			discovery.awaitUninterruptibly();
			bootstrapServerPeerAddress = discovery.reporter();
			FutureBootstrap bootstrap = peer.bootstrap().peerAddress(bootstrapServerPeerAddress).start();
			bootstrap.awaitUninterruptibly();

		    PeerDHT myPeerDHT = new PeerBuilderDHT(peer).start();

	        FuturePut putFuture = myPeerDHT.put(Number160.createHash("key")).data(new Data("Hello World")).start();
	        putFuture.awaitUninterruptibly();
	        FutureGet futureDHT = myPeerDHT.get(Number160.createHash("key")).start();
	        futureDHT.awaitUninterruptibly();
	        futureDHT.futureRequests().awaitUninterruptibly();
	        Data data = futureDHT.data();
	        if (data == null) {
	            throw new RuntimeException("Address not available in DHT.");
	        } 
	        System.err.println("returned " + data.object());
		}
	}
}
