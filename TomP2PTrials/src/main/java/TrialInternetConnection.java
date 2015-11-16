import java.io.IOException;
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
import utils.GetOwnIpAddressTest;

public class TrialInternetConnection {
	public static void main(String[] args) throws InterruptedException {

		String ip = "192.168.43.234";
		int port = 4000;
		int peerID =3;
		if (peerID == 1) {
			Bindings b = new Bindings();
			Random RND = new Random();
			b.addInterface("wlan0");

			try {
				GetOwnIpAddressTest.main(null);
				Peer start = new PeerBuilder(Number160.createHash("super peer")).bindings(b).ports(port).broadcastHandler(new MyBroadcastHandler())
						.behindFirewall(true).start();
				System.out.println("Created peer");
				PeerDHT peerDHT = new PeerBuilderDHT(start).start();
				System.out.println("Created peerDHT");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {

			Bindings b = new Bindings();
			b.addInterface("wlan0");
			try {
				Peer peer = new PeerBuilder(Number160.createHash("client peer")).bindings(b).ports(port + 1).behindFirewall(true)
						.broadcastHandler(new MyBroadcastHandler()).start();
				PeerAddress bootstrap = new PeerAddress(Number160.ZERO, new InetSocketAddress(ip, port));
				FutureDiscover futureDiscover = peer.discover().peerAddress(bootstrap).start();
				futureDiscover.awaitUninterruptibly();
				if (futureDiscover.isSuccess()) {
					System.out.println("Successfully discovered");
					InetSocketAddress myInetSocketAddress = new InetSocketAddress(peer.peerAddress().inetAddress(), port);

					bootstrap = futureDiscover.reporter();
					FutureBootstrap fb = peer.bootstrap().peerAddress(bootstrap).start();
					fb.awaitUninterruptibly();

					PeerDHT peerDHT = new PeerBuilderDHT(peer).start();
					if (fb.isSuccess()) {
						Thread.sleep(4000);
						String key = "Hello World";
						if (peerID == 2) {
//							NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
//							Number160 hash = Number160.createHash("Hello World Broadcast");
//							dataMap.put(new Number640(hash, hash, hash, hash), new Data("Hello World Broadcast"));
//							peer.broadcast(hash).dataMap(dataMap).start();
							FuturePut start = peerDHT.put(Number160.createHash(key)).data(new Data(key+" 1")).start();
							start.addListener(new BaseFutureListener<FuturePut>() {

								@Override
								public void operationComplete(FuturePut future) throws Exception {
									if(future.isSuccess()){
										System.out.println("Successful put");
									}
								}

								@Override
								public void exceptionCaught(Throwable t) throws Exception {
									t.printStackTrace();
								}
							});
						}else if(peerID == 3){
							FutureGet get = peerDHT.get(Number160.createHash(key)).start();
							get.addListener(new BaseFutureListener<FutureGet>() {

								@Override
								public void operationComplete(FutureGet future) throws Exception {
									if(future.isSuccess()){
										System.out.println("Got: " +future.data().object());
									}
								}

								@Override
								public void exceptionCaught(Throwable t) throws Exception {
									t.printStackTrace();
								}
							});
						}
					}
				} else {
					System.out.println("Could not connect");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
