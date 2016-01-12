package mapreduce.execution.task;

import java.io.Serializable;

import mapreduce.execution.IDomain;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public class Task extends AbstractFinishable implements Serializable, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4696324648240806323L;
	/** Key of this task to get the values for */
	private final String key;
	/** Set true if this tasks's result keys and values were successfully transferred from executor task domain to executor job procedure domain */
	private volatile boolean isInProcedureDomain = false;
	/** Used in the scheduler to not schedule too many executions of the same task */
	private volatile int activeCount = 0;
	/** Just a counter to be used in the executor task domains */
	private volatile int statusIndex = 0;

	public Task decrementActiveCount() {
		if (this.activeCount > 0) {
			--this.activeCount;
		}
		return this;
	}

	public Task incrementActiveCount() {
		if (canBeExecuted()) {
			++this.activeCount;
		}
		return this;
	}

	public boolean canBeExecuted() {
		return currentMaxNrOfSameResultHash() + activeCount < nrOfSameResultHash;
	}

	public Integer activeCount() {
		return activeCount;
	}

	/**
	 * Earlier, this had a meaning. Now it's only there to tell apart the execution, if the same executor executes the task multiple times
	 * 
	 * @return
	 */
	public int newStatusIndex() {
		return this.statusIndex++;
	}

	private Task(String key) {
		this.key = key;
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
	public Number160 calculateResultHash() {
		if (resultOutputDomain == null) {
			checkIfFinished();
		}
		return (resultOutputDomain == null ? null : resultOutputDomain.resultHash());
	}

	public void reset() {
		outputDomains.clear();
		resultOutputDomain = null;
	}

	@Override
	public Task nrOfSameResultHash(int nrOfSameResultHash) {
		return (Task) super.nrOfSameResultHash(nrOfSameResultHash);
	}

	@Override
	public Task addOutputDomain(IDomain domain) {
		return (Task) super.addOutputDomain(domain);
	}

	@Override
	public String toString() {
		return "Task [key=" + key + ", isInProcedureDomain=" + isInProcedureDomain + " " + super.toString() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
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
		Task other = (Task) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
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

	public int nrOfSameResultHash() {
		// TODO Auto-generated method stub
		return nrOfSameResultHash;
	}

}
