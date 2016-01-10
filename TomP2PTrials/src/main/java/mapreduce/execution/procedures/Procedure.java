package mapreduce.execution.procedures;

import java.io.Serializable;
import java.util.List;

import mapreduce.execution.IDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.task.AbstractFinishable;
import mapreduce.execution.task.Task;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public final class Procedure extends AbstractFinishable implements Serializable, Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1717123684693430690L;
	private final IExecutable procedure;
	private final int procedureIndex;

	private List<Task> tasks;
	/** Location of keys for this procedure */
	private JobProcedureDomain inputDomain;
	/** Used to combine data before it is sent to the dht. Local aggregation */
	private IExecutable combiner;
	private int nrOfSameResultHashForTasks;

	private Procedure(IExecutable procedure, int procedureIndex) {
		this.procedure = procedure;
		this.procedureIndex = procedureIndex;
		this.tasks = SyncedCollectionProvider.syncedArrayList();
		this.inputDomain = null;
	}

	public static Procedure create(IExecutable procedure, int procedureIndex) {
		return new Procedure(procedure, procedureIndex);
	}

	public Procedure inputDomain(JobProcedureDomain inputDomain) {
		this.inputDomain = inputDomain;
		return this;
	}

	public JobProcedureDomain inputDomain() {
		return this.inputDomain;
	}

	public IExecutable executable() {
		return procedure;
	} 

	@Override
	public Number160 calculateResultHash() {
		Number160 resultHash = Number160.ZERO;
		for (Task task : tasks) {
			Number160 taskResultHash = task.calculateResultHash();
			if (taskResultHash == null) {
				return null;
			} else {
				resultHash = resultHash.xor(taskResultHash);
			}
		}
		return resultHash;
	}

	public int procedureIndex() {
		return this.procedureIndex;
	}

	public List<Task> tasks() {
		return this.tasks;
	}

	public Procedure addTask(Task task) {
		synchronized (this.tasks) {
			if (!this.tasks.contains(task)) {
				task.nrOfSameResultHash(nrOfSameResultHashForTasks);
				this.tasks.add(task);
			}
		}
		return this;
	}

	public Procedure tasks(List<Task> tasks) {
		this.tasks.clear();
		this.tasks.addAll(tasks);
		updateNrOfSameResultHash();
		return this;
	}

	public int nrOfFinishedTasks() {
		int finishedTasksCounter = 0;
		for (Task task : tasks) {
			if (task.isFinished()) {
				++finishedTasksCounter;
			}
		}
		return finishedTasksCounter;
	}

	private void updateNrOfSameResultHash() {
		for (Task task : tasks) {
			task.nrOfSameResultHash(nrOfSameResultHashForTasks);
		}
	}

	@Override
	public String toString() {
		return "Procedure [procedure=" + procedure + ", procedureIndex=" + procedureIndex + ", tasks=" + tasks + ", inputDomain=" + inputDomain + "]";
	}

	public void reset() {
		for (Task task : tasks) {
			task.reset();
		}
	}

	@Override
	public Procedure nrOfSameResultHash(int nrOfSameResultHash) {
		return (Procedure) super.nrOfSameResultHash(nrOfSameResultHash);
	}

	public Procedure nrOfSameResultHashForTasks(int nrOfSameResultHashForTasks) {
		this.nrOfSameResultHashForTasks = nrOfSameResultHashForTasks;
		updateNrOfSameResultHash();
		return this;
	}

	// @Override
	// public int nrOfOutputDomains() {
	// return outputDomains.size();
	// }

	@Override
	public Procedure clone() {
		Procedure procedure = null;
		try {
			procedure = (Procedure) super.clone();
			return procedure;
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((procedure == null) ? 0 : procedure.hashCode());
		result = prime * result + procedureIndex;
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
		Procedure other = (Procedure) obj;
		if (procedure == null) {
			if (other.procedure != null)
				return false;
		} else if (!procedure.equals(other.procedure))
			return false;
		if (procedureIndex != other.procedureIndex)
			return false;
		return true;
	}

	public static void main(String[] args) {
		Procedure p = Procedure.create(WordCountReducer.create(), 2);
		Procedure p2 = p.clone();
		System.out.println(p);
		System.out.println(p2);
	}

	public IExecutable combiner() {
		return this.combiner;
	}

	public Procedure combiner(IExecutable combiner) {
		this.combiner = combiner;
		return this;
	}
}
