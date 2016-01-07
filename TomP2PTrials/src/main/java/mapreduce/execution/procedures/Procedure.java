package mapreduce.execution.procedures;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.IDomain;
import mapreduce.execution.IFinishable;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.task.Task;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public final class Procedure implements IFinishable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1717123684693430690L;
	private final IExecutable procedure;
	private final int procedureIndex;

	private List<Task> tasks;
	/** Location of keys for this procedure */
	private JobProcedureDomain inputDomain;
	/** Location of keys for next procedure */
	private List<JobProcedureDomain> outputDomains;
	/** How many times this object needs to be executed before it is declared finished */
	private int nrOfSameResultHash = 1;
	/** Specifies if the task is currently executed */
	private volatile boolean isActive;
	/** Number of tasks for this procedure (may be different from tasks.size() because tasks are pulled after another and not all at the same time)*/
	private int tasksSize;
	private JobProcedureDomain resultOutputDomain;

	private Procedure(IExecutable procedure, int procedureIndex) {
		this.procedure = procedure;
		this.procedureIndex = procedureIndex;
		this.tasks = SyncedCollectionProvider.syncedArrayList();
		this.outputDomains = SyncedCollectionProvider.syncedArrayList();
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

	public boolean isActive() {
		return this.isActive;
	}

	public Procedure isActive(boolean isActive) {
		this.isActive = isActive;
		return this;
	}

	public boolean isFinished() {
		return resultOutputDomain != null;
	}

	@Override
	public Procedure addOutputDomain(IDomain domain) {
		this.outputDomains.add((JobProcedureDomain) domain);
		return this;

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
				this.tasks.add(task.nrOfSameResultHash(nrOfSameResultHash));
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

	private void updateNrOfSameResultHash() {
		for(Task task: tasks) {
			task.nrOfSameResultHash(nrOfSameResultHash);
		}
	}

	public Procedure tasksSize(int tasksSize) {
		this.tasksSize = tasksSize;
		return this;
	}

	public int tasksSize() {
		return this.tasksSize;
	}

	@Override
	public Number160 calculateResultHash() {
		Number160 resultHash = Number160.ZERO;
		for (Task task : tasks) {
			resultHash = resultHash.xor(task.calculateResultHash());
		}
		return resultHash;
	}

	@Override
	public JobProcedureDomain resultOutputDomain() {
		ListMultimap<Number160, JobProcedureDomain> results = ArrayListMultimap.create();
		for (JobProcedureDomain domain : outputDomains) {
			results.put(domain.resultHash(), domain);
		}
		for (Number160 resultHash : results.keySet()) {
			if (results.get(resultHash).size() >= nrOfSameResultHash) {
				this.resultOutputDomain = results.get(resultHash).get(0);
				break;
			}
		}
		return resultOutputDomain;
	}

	public void reset() {
		for (Task task : tasks) {
			task.reset();
		}
	}

	@Override
	public Procedure nrOfSameResultHash(int nrOfSameResultHash) {
		this.nrOfSameResultHash = nrOfSameResultHash;
		updateNrOfSameResultHash();
		return this;
	}

	@Override
	public int nrOfOutputDomains() {
		return outputDomains.size();
	}
}
