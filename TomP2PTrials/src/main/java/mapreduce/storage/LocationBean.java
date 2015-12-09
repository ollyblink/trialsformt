package mapreduce.storage;

import java.io.Serializable;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class LocationBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1337132212823970081L;
	private int jobStatusIndex;
	private String procedureSimpleName;
	private Number160 peerId;
	private IMapReduceProcedure procedure;
	private Tuple<PeerAddress, Integer> dataLocation;

	public static LocationBean create(final Tuple<PeerAddress, Integer> dataLocation, final IMapReduceProcedure procedure) {
		return new LocationBean().peerId(dataLocation.first().peerId()).jobStatusIndex(dataLocation.second())
				.procedureSimpleName(procedure.getClass().getSimpleName()).dataLocation(dataLocation).procedure(procedure);
	}

	private LocationBean procedure(IMapReduceProcedure procedure) {
		this.procedure = procedure;
		return this;
	}

	private LocationBean dataLocation(Tuple<PeerAddress, Integer> dataLocation) {
		this.dataLocation = dataLocation;
		return this;
	}

	public IMapReduceProcedure procedure() {
		return this.procedure;
	}

	public Tuple<PeerAddress, Integer> dataLocation() {
		return this.dataLocation;
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

	public String domain(String taskId) {
		return taskId + procedureSimpleName + peerId + jobStatusIndex;
	}

	@Override
	public String toString() {
		return "LocationBean [jobStatusIndex=" + jobStatusIndex + ", procedureSimpleName=" + procedureSimpleName + ", peerId=" + peerId + "]";
	}

	public int jobStatusIndex() {
		return jobStatusIndex;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataLocation == null) ? 0 : dataLocation.hashCode());
		result = prime * result + ((procedure == null) ? 0 : procedure.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LocationBean other = (LocationBean) obj;
		if (dataLocation == null) {
			if (other.dataLocation != null)
				return false;
		} else if (!dataLocation.equals(other.dataLocation))
			return false;
		if (procedure == null) {
			if (other.procedure != null)
				return false;
		} else if (!procedure.equals(other.procedure))
			return false;
		return true;
	}

}
