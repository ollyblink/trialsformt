package mapreduce.engine.broadcasting;

import java.util.Random;

import org.junit.Test;

import mapreduce.engine.messageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;

public class MRBroadcastHandlerTest {
	private Random random = new Random();

	@Test
	public void test() throws Exception {
		MRJobExecutionManagerMessageConsumer messageConsumer = MRJobExecutionManagerMessageConsumer.create();
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 1, messageConsumer);
		messageConsumer.dhtConnectionProvider(dhtConnectionProvider);
		MRBroadcastHandler handler = dhtConnectionProvider.broadcastHandler();
		handler.addBCMessage(bcMessage);
	}

}
