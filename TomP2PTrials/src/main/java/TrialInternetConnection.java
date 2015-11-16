import java.io.IOException;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

public class TrialInternetConnection {
	public static void main(String[] args) {
		Bindings b = new Bindings();
		Random RND = new Random();
		b.addInterface("wlan1");
		int port = 4000;
		String ip = "192.168.43.65";
		try {
	        Peer start = new PeerBuilder(Number160.createHash("super peer")).ports(port).start();
	        PeerDHT peerDHT = new PeerBuilderDHT(start).start();
 		} catch (IOException e) { 
			e.printStackTrace();
		}
	}
}
