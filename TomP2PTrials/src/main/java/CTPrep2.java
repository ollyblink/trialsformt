import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import firstdesignidea.execution.computation.context.IContext;
import firstdesignidea.execution.computation.mapper.IMapper;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.BroadcastHandler;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.BroadcastRPC;
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
		// Random random = new Random();

		// int firstIndex = random.nextInt(nrOfPeers);
		Set<String> keys = new TreeSet<String>();

		IMapper<String, String, String, String> mapper = new IMapper<String, String, String, String>() {

			@Override
			public void map(String key, String value, IContext<String, String> context) {
				String[] split = value.split(" ");
				for (String s : split) {
					context.write(s, "1");
				}
			}

		};

		try {
			// String[] words = "hello there hello world hello hello how low".split(" ");
			// for (String word : words) {
			// keys.add(word);
			// final String j = word;
			// System.err.println("a-(" + word + ")");
			FuturePut futurePut = master.put(Number160.createHash("Mapper")).data(new Data(mapper)).start();
			futurePut.addListener(new BaseFutureListener<FuturePut>() {

				@Override
				public void operationComplete(FuturePut future) throws Exception {
					// System.out.println("added: f " + j + " (" + future.isSuccess() + ")");
					// System.out.println(future.failedReason());
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
			// Thread.sleep(500);
			// }

			Thread.sleep(2000);
			// for (String key : keys) {
			// System.err.println("b-(" + key + ")");
			FutureGet futureGet = master.get(Number160.createHash("Mapper")).start();
			futureGet.addListener(new BaseFutureListener<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {
					// System.out.println("size " + future.dataMap().size());
					// Iterator<Data> iterator = future.dataMap().values().iterator();
					// while (iterator.hasNext()) {
					// System.out.println(iterator.next().object());
					// }  
					if (future.data().object() instanceof IMapper) {
						IMapper<String, String, String, String> mapper = (IMapper<String, String, String, String>) future.data().object();

						mapper.map("trial", "test test this is a test", new IContext<String, String>() {

							@Override
							public void write(String keyOut, String valueOut) {
								System.out.println(keyOut + ": " + valueOut);
							}

						});
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
				Thread.sleep(1000);
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
