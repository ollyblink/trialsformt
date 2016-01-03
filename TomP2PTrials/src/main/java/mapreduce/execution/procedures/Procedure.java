package mapreduce.execution.procedures;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.IDomain;
import mapreduce.execution.IFinishable;
import mapreduce.execution.task.Task2;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public final class Procedure implements IFinishable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1717123684693430690L;
	private final IExecutable procedure;
	private final int procedureIndex;

	private List<Task2> tasks;
	/** Location of keys for this procedure */
	private JobProcedureDomain inputDomain;
	/** Location of keys for next procedure */
	private List<IDomain> outputDomains;
	/** How many times this object needs to be executed before it is declared finished */
	private int nrOfSameResultHash;
	/** Specifies if the task is currently executed */
	private volatile boolean isActive;
	private int tasksSize;

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
		boolean isFinished = true;
		ListMultimap<IDomain, Number160> results = ArrayListMultimap.create();
		for (IDomain domain : outputDomains) {
			results.put(domain, domain.resultHash());
		}
		for (IDomain domain : results.keySet()) {
			if (results.get(domain).size() >= nrOfSameResultHash) {
				isFinished = true;
				break;
			}
		}
		return isFinished;
	}

	@Override
	public void addOutputDomain(IDomain domain) {
		this.outputDomains.add(domain);

	}

	public int procedureIndex() {
		return this.procedureIndex;
	}

	public List<Task2> tasks() {
		return this.tasks;
	}

	public Procedure addTask(Task2 task) {
		synchronized (this.tasks) {
			if (!this.tasks.contains(task)) {
				this.tasks.add(task);
			}
		}
		return this;
	}

	public Procedure tasks(List<Task2> tasks) {
		this.tasks.clear();
		this.tasks.addAll(tasks);
		return this;
	}

	public void tasksSize(int tasksSize) {
		this.tasksSize = tasksSize;
	}

	public int tasksSize() {
		return this.tasksSize;
	}

	@Override
	public List<IDomain> outputDomains() { 
		return this.outputDomains;
	}
}
