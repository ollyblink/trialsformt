package firstdesignidea.execution.jobtask;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import firstdesignidea.execution.computation.IMapReduceProcedure;
import net.tomp2p.peers.PeerAddress;

public class Task implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5374181867289486399L;

	private String id;
	private String jobId;
	private IMapReduceProcedure<?, ?, ?, ?> procedure;
	private List<?> keys;
	private int procedureIndex;
	private Map<PeerAddress, JobStatus> executingPeers;

	private Task() {
		executingPeers = new HashMap<PeerAddress, JobStatus>();
	}

	public static Task newTask() {
		return new Task();
	}

	public String id() {
		return procedureIndex + "_" + id;
	}

	public String jobId() {
		return jobId;
	}

	public IMapReduceProcedure<?, ?, ?, ?> procedure() {
		return this.procedure;
	}

	public List<?> keys() {
		return this.keys;
	}

	public Task keys(List<?> keys) {
		this.keys = keys;
		return this;
	}

	public Task id(String id) {
		this.id = id;
		return this;
	}

	public Task jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	/**
	 * 
	 * @param procedure
	 * @param procedureIndex
	 *            specifies which procedure in the queue it is, used for task id
	 * @return
	 */
	public Task procedure(IMapReduceProcedure<?, ?, ?, ?> procedure, int procedureIndex) {
		this.procedure = procedure;
		this.procedureIndex = procedureIndex;
		return this;
	}

	public Task updateExecutingPeerStatus(PeerAddress peerAddress, JobStatus currentStatus) {
		this.executingPeers.put(peerAddress, currentStatus);
		return this;
	}

	/**
	 * Check how many peers are currently executing, finished, or failed to execute this task by specifying the status to check as an argument
	 * 
	 * @param statusToCheck
	 *            <code>JobStatus</code> to check how many peers for this task are currently holding it
	 * @return number of peers that were assigned this task and currently hold the specified <code>JobStatus</code>
	 */
	public int numberOfPeersWithStatus(JobStatus statusToCheck) {
		int nrOfPeers = 0;
		for (PeerAddress executingPeer : this.executingPeers.keySet()) {
			JobStatus status = this.executingPeers.get(executingPeer);
			if (status.equals(statusToCheck)) {
				++nrOfPeers;
			}
		}
		return nrOfPeers;
	}

	public int numberOfAssignedPeers() {
		return this.executingPeers.keySet().size();
	}
}
