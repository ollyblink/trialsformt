package generictests;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;

public class MyBroadcastHandler extends StructuredBroadcastHandler {

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		return super.receive(message);
	}

}
