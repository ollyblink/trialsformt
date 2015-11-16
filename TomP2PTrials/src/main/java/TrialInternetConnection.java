import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Random;

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
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import utils.GetOwnIpAddressTest;

public class TrialInternetConnection {
	public static void main(String[] args) throws InterruptedException, IOException {
		final String key = "Hello World";

		String ip = "192.168.43.234";
		int port = 4000;
		int peerID = 3;
		GetOwnIpAddressTest.main(null);
		if (peerID == 1) {
			Bindings b = new Bindings();
			Random RND = new Random();
			b.addInterface("wlan0");

			Peer start = new PeerBuilder(Number160.createHash("super peer")).bindings(b).ports(port).broadcastHandler(new MyBroadcastHandler())
					.behindFirewall(true).start();
			final PeerDHT peerDHT = new PeerBuilderDHT(start).start();

		} else {

			Bindings b = new Bindings();
			b.addInterface("wlan0");

			Peer peer = new PeerBuilder(Number160.createHash("client peer")).bindings(b).ports(port + 2).behindFirewall(true)
					.broadcastHandler(new MyBroadcastHandler()).start();
			PeerAddress bootstrap = new PeerAddress(Number160.ZERO, new InetSocketAddress(InetAddress.getByName(ip), port));
			FutureDiscover futureDiscover = peer.discover().peerAddress(bootstrap).start();
			futureDiscover.awaitUninterruptibly();

		}
	}
}
