package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class FinishedTaskComparionsBCMessage extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6193571212837570134L;
	private Tuple<PeerAddress, Integer> finalDataLocation;
	private String taskId;

	@Override
	public BCStatusType status() {
		return BCStatusType.FINISHED_TASK_COMPARISON;
	}

	@Override
	public void execute(IMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedTaskComparion(jobId, taskId, finalDataLocation);
	}

	private FinishedTaskComparionsBCMessage() {

	}

	public static FinishedTaskComparionsBCMessage newInstance() {
		return new FinishedTaskComparionsBCMessage();
	}

	public FinishedTaskComparionsBCMessage taskId(String taskId) {
		this.taskId = taskId;
		return this;
	}

	public FinishedTaskComparionsBCMessage finalDataLocation(Tuple<PeerAddress, Integer> finalDataLocation) {
		this.finalDataLocation = finalDataLocation;
		return this;
	}
}
