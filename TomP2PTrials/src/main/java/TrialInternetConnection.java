import java.io.IOException;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import utils.GetOwnIpAddressTest;

public class TrialInternetConnection {
	public static void main(String[] args) {

		int peerID = 1;
		if (peerID == 1) {
			Bindings b = new Bindings();
			Random RND = new Random();
			b.addInterface("wlan0");

			String ip = "192.168.43.234";
			int port = 4000;
			try {
				GetOwnIpAddressTest.main(null);
				Peer start = new PeerBuilder(Number160.createHash("super peer")).bindings(b).ports(port).behindFirewall(true).start();
				System.out.println("Created peer");
				PeerDHT peerDHT = new PeerBuilderDHT(start).start();
				System.out.println("Created peerDHT");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {

		}
	}
}
