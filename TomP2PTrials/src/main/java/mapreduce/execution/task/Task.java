package mapreduce.execution.task;

import java.io.Serializable;
import java.util.List;

import mapreduce.execution.IDomain;
import mapreduce.utils.SyncedCollectionProvider;

public class Task extends AbstractFinishable implements Serializable, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4696324648240806323L;
	/** Key of this task to get the values for */
	private final String key;
	/** Set true if this tasks's result keys and values were successfully transferred from executor task domain to executor job procedure domain */
	private volatile boolean isInProcedureDomain;
	/** Specifies local execution assignments */
	private List<String> assignedExecutors;

	private Task(String key) {
		this.key = key;
		this.assignedExecutors = SyncedCollectionProvider.syncedArrayList();
	}

	public static Task create(String key) {
		return new Task(key);
	}

	public String key() {
		return this.key;
	}

	public boolean isInProcedureDomain() {
		return this.isInProcedureDomain;
	}

	public Task isInProcedureDomain(boolean isInProcedureDomain) {
		this.isInProcedureDomain = isInProcedureDomain;
		return this;
	}

	@Override
	protected Task clone() {

		try {
			Task task = (Task) super.clone();
			task.outputDomains = SyncedCollectionProvider.syncedArrayList();
			for (IDomain e : outputDomains) {
				task.outputDomains.add((IDomain) e.clone());
			}
			task.resultOutputDomain = (IDomain) resultOutputDomain.clone();
			return task;
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public int nextStatusIndexFor(String executor) {
		int counter = 0;
		for (String e : assignedExecutors) {
			if (e.equals(executor)) {
				++counter;
			}
		}
		return counter;
	}

	public Task addAssignedExecutor(String executor) {
		this.assignedExecutors.add(executor);
		return this;
	}

	public void reset() {
		outputDomains.clear();
		resultOutputDomain = null;
	}

	@Override
	public String toString() {
		return "Task [key=" + key + ", isInProcedureDomain=" + isInProcedureDomain + "]";
	}

	public int nrOfAssignedWorkers() {
		return this.assignedExecutors.size();
	}
}
