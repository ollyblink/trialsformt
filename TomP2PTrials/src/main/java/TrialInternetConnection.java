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

public class TrialInternetConnection {
	public static void main(String[] args) {
		Bindings b = new Bindings();
		Random RND = new Random();
		b.addInterface("wlan1");
		int port = 4000;
		String ip = "192.168.43.65";
		try {
	        Peer peer = new PeerBuilder(Number160.createHash("client peer")).bindings(b).ports(port).behindFirewall(true).start();
	        PeerDHT peerDHT = new PeerBuilderDHT(peer).start();
	        PeerAddress bootstrap = new PeerAddress(Number160.ZERO, new InetSocketAddress("192.168.43.234", port));
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
