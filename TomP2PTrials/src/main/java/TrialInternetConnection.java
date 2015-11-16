import java.io.IOException;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

public class TrialInternetConnection {
	public static void main(String[] args) {
		Bindings b = new Bindings();
		Random RND = new Random();
		b.addInterface("wlan0");
		
		String ip = "192.168.43.234";
		try {
			PeerDHT peer = new PeerBuilderDHT(new PeerBuilder(new Number160(RND)).ports(4000).bindings(b).start()).start();
			
		} catch (IOException e) { 
			e.printStackTrace();
		}
	}
}
