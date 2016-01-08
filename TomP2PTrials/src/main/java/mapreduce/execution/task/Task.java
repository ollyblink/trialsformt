package mapreduce.execution.task;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.IDomain;
import mapreduce.execution.IFinishable;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public class Task extends AbstractFinishable implements Serializable, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4696324648240806323L;
	/** Key of this task to get the values for */
	private final String key;
	/** Specifies if the task is currently executed */
	private volatile boolean isActive;
	/** Set true if this tasks's result keys and values were successfully transferred from executor task domain to executor job procedure domain */
	private volatile boolean isInProcedureDomain;

	private Task(String key) {
		this.key = key;
	}

	public static Task create(String key) {
		return new Task(key);
	}

	public String key() {
		return this.key;
	}

	public boolean isActive() {
		return this.isActive;
	}

	public Task isActive(boolean isActive) {
		this.isActive = isActive;
		return this;
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
		for (IDomain outputDomain : outputDomains) {
			if (outputDomain.executor().equals(executor)) {
				++counter;
			}
		}
		return counter;
	}

	public void reset() {
		outputDomains.clear();
		resultOutputDomain = null;
	}
}
