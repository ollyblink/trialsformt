import java.io.IOException;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;

public class CTPrep2 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		PeerDHT[] peers = null;
		int nrOfPeers = 100;
		int port = 4001;

		// final String myPhoneNumber = "079 666 40 20";
		// final String myName = "Oliver Zihler";
		peers = Example.createAndAttachPeersDHT(nrOfPeers, port);

		Example.bootstrap(peers);

		Thread.sleep(1000);
		final PeerDHT master = peers[0];
		Set<String> keys = new TreeSet<String>();

		try {
			Random RND = new Random();
			for (int i = 0; i < 5; ++i) {

				FuturePut futurePut = peers[RND.nextInt(100)].add(Number160.createHash("Mapper")).data(new Data("Olly" + i)).start();
				futurePut.addListener(new BaseFutureListener<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							System.err.println("send message mapper with bc");
							TreeMap<Number640, Data> treeMap = new TreeMap<Number640, Data>();
							treeMap.put(new Number640(Number160.createHash("Mapper"), Number160.createHash("Mapper"), Number160.createHash("Mapper"),
									Number160.createHash("Mapper")), new Data("added some mapper to the dht"));
							master.peer().broadcast(Number160.createHash("Mapper")).dataMap(treeMap).start();

						}
					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						t.printStackTrace();
					}
				});
			}
			// Thread.sleep(500);
			// }

			Thread.sleep(2000);
			// for (String key : keys) {
			// System.err.println("b-(" + key + ")");
			FutureGet futureGet = master.get(Number160.createHash("Mapper")).all().start();
			futureGet.addListener(new BaseFutureListener<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {

					if (future.isSuccess()) { 
						Set<Entry<Number640, Data>> entrySet = future.dataMap().entrySet();
						for (Entry<Number640, Data> e : entrySet) {
							System.out.println(e.getValue().object());
						}
					} else {
						System.err.println("No success");
					}
				}

				@Override
				public void exceptionCaught(Throwable t) throws Exception {
					t.printStackTrace();
				}
			});
			// }

		} finally {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			master.shutdown();
		}
	}

	private static void trials(PeerDHT[] peers, int nrOfPeers, final String myName, Random random, int firstIndex) throws IOException {
		for (final PeerDHT p : peers) {

			p.peer().objectDataReply(new ObjectDataReply() {

				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					System.out.println("I am " + p.peerID() + " and received a message from : " + sender.peerId() + " sent: " + request);
					return "reply";
				}
			});
		}
		final PeerDHT peer1 = peers[firstIndex];
		FuturePut futurePut = peers[firstIndex].put(Number160.createHash(myName)).data(new Data(peers[firstIndex].peerAddress())).start();
		futurePut.addListener(new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					System.out.println("Put successful");
					System.out.println("Stored: " + peer1.peerAddress());
				} else {
					System.out.println("Could not put");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				System.out.println("PUT EXCEPTION");
				System.out.println(t);
			}
		});

		int secondIndex = random.nextInt(nrOfPeers);
		while (secondIndex == firstIndex) {
			secondIndex = random.nextInt(nrOfPeers);
		}
		final PeerDHT peer2 = peers[secondIndex];

		FutureGet futureGet = peer2.get(Number160.createHash("Hans")).start();
		futureGet.addListener(new BaseFutureListener<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					if (future.data() != null) {
						PeerAddress peerAddress = ((PeerAddress) future.data().object());
						System.out.println("GET successful");
						System.out.println("Found " + peerAddress);
						System.out.println("Send hi");

						FutureDirect futureDirect = peer2.peer().sendDirect(peerAddress).object("hi").start();
						futureDirect.addListener(new BaseFutureListener<FutureDirect>() {

							@Override
							public void operationComplete(FutureDirect future) throws Exception {
								if (future.isSuccess()) {
									System.out.println("Received: " + future.object());
								} else {
									System.out.println("No success on send direct");
								}
							}

							@Override
							public void exceptionCaught(Throwable t) throws Exception {
								System.out.println("Direct EXCEPTION");
								System.out.println(t);
							}
						});
					} else {
						System.out.println("Could not find cause its null" + new String(myName));

					}
				} else {
					System.out.println("Could not find " + new String(myName));
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				System.out.println("GET EXCEPTION");
				System.out.println(t);
			}

		});
	}

}
