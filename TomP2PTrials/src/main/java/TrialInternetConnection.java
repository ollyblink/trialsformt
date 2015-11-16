import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import utils.GetOwnIpAddressTest;

public class TrialInternetConnection {
	public static void main(String[] args) {

		String ip = "192.168.43.234";
		int port = 4000;
		int peerID = 2;
		String ip = "192.168.43.234";
		int port = 4000;
		if (peerID == 1) {
			Bindings b = new Bindings();
			Random RND = new Random();
			b.addInterface("wlan0");

			try {
				GetOwnIpAddressTest.main(null);
				Peer start = new PeerBuilder(Number160.createHash("super peer")).bindings(b).ports(port).broadcastHandler(new MyBroadcastHandler()).behindFirewall(true).start();
				System.out.println("Created peer");
				PeerDHT peerDHT = new PeerBuilderDHT(start).start();
				System.out.println("Created peerDHT");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {

		Bindings b2 = new Bindings(); 
		b2.addInterface("wlan1"); 
		String ip2 = "192.168.43.65";
		try {
	        Peer peer = new PeerBuilder(Number160.createHash("client peer")).bindings(b2).ports(port).behindFirewall(true).broadcastHandler(new MyBroadcastHandler()).start();
	        PeerDHT peerDHT = new PeerBuilderDHT(peer).start();
	        PeerAddress bootstrap = new PeerAddress(Number160.ZERO, new InetSocketAddress(ip, port));
	        FutureDiscover futureDiscover = peer.discover().peerAddress(bootstrap).start();
	        futureDiscover.awaitUninterruptibly();
	        if(futureDiscover.isSuccess()){
	        	System.out.println("Successfully connected");
	        }else{
	        	System.out.println("Could not connect");
	        }
 		} catch (IOException e) { 
			e.printStackTrace();
		}
		}
	}
}
