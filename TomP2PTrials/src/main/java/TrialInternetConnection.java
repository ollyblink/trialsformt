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
		String ipSuperPeer = "192.168.43.234";
		int port = 4000;
		int peerID = 2;
		GetOwnIpAddressTest.main(null);
		if (peerID == 1) {
//			Bindings b = new Bindings();
//			Random RND = new Random();
//			b.addInterface("wlan0");

	        new PeerBuilder(Number160.createHash("super peer")).ports(port).start();

		} else {
			Peer myPeer = new PeerBuilder(Number160.createHash("client peer")).behindFirewall(true).ports(port).enableMaintenance(false).start();
			PeerAddress bootstrapServerPeerAddress = new PeerAddress(Number160.ZERO, new InetSocketAddress(InetAddress.getByName(ipSuperPeer), port));

			FutureDiscover discovery = myPeer.discover().peerAddress(bootstrapServerPeerAddress).start();
			discovery.awaitUninterruptibly();
			if (!discovery.isSuccess()) {
				System.err.println("no success!");
			}
			System.err.println("Peer: " + discovery.reporter() + " told us about our address.");
			InetSocketAddress myInetSocketAddress = new InetSocketAddress(myPeer.peerAddress().inetAddress(), port);

			bootstrapServerPeerAddress = discovery.reporter();
			FutureBootstrap bootstrap = myPeer.bootstrap().peerAddress(bootstrapServerPeerAddress).start();
			bootstrap.awaitUninterruptibly();

			if (!bootstrap.isSuccess()) {
				System.err.println("no success!");
			}

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
			myPeer.shutdown();
		}
	}
}