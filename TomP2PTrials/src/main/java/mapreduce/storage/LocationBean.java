package mapreduce.storage;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.jobtask.Task;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public final class LocationBean {

	private int jobStatusIndex;
	private String procedureSimpleName;
	private Number160 peerId;

	public static LocationBean newInstance(final Tuple<PeerAddress, Integer> initialDataLocation, final IMapReduceProcedure procedure) {
		return new LocationBean().peerId(initialDataLocation.first().peerId()).jobStatusIndex(initialDataLocation.second())
				.procedureSimpleName(procedure.getClass().getSimpleName());
	}

	private LocationBean procedureSimpleName(String procedureSimpleName) {
		this.procedureSimpleName = procedureSimpleName;
		return this;
	}

	private LocationBean jobStatusIndex(int jobStatusIndex) {
		this.jobStatusIndex = jobStatusIndex;
		return this;
	}

	private LocationBean peerId(Number160 peerId) {
		this.peerId = peerId;
		return this;
	}

	public String domain(final Task task) {
		return task.id() + procedureSimpleName + peerId + jobStatusIndex;
	}

}
