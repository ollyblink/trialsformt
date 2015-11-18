package firstdesignidea.execution.broadcasthandler;

import java.io.IOException;
import java.util.NavigableMap;

import firstdesignidea.execution.jobtask.IJobManager;
import firstdesignidea.execution.jobtask.JobStatus;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MRBroadcastHandler extends StructuredBroadcastHandler {

	private IJobManager jobManager;

	public MRBroadcastHandler(IJobManager jobManager) {
		this.jobManager = jobManager;
	}

	public MRBroadcastHandler() {

	}

	public MRBroadcastHandler jobManager(IJobManager jobManager) {
		this.jobManager = jobManager;
		return this;
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {
 		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				JobStatus jobStatus = (JobStatus) dataMap.get(nr).object();

				switch (jobStatus) {
				case DISTRIBUTED_TASKS:
					break;
				case FINISHED_ALL_TASKS:
					break;
				default:
					break;
				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // to get from message
		return super.receive(message);
	}

}
