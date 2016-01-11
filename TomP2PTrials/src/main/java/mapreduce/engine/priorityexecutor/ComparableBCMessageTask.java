package mapreduce.engine.priorityexecutor;

import java.util.concurrent.FutureTask;

import mapreduce.engine.broadcasting.BCMessageStatus;
import mapreduce.execution.job.PriorityLevel;

public class ComparableBCMessageTask<T> extends FutureTask<T> implements Comparable<ComparableBCMessageTask<T>> {
	private volatile PriorityLevel jobPriority;
	private volatile Long jobCreationTime;
	private volatile BCMessageStatus messageStatus;
	private volatile Long messageCreationTime;
	private volatile Integer procedureIndex;

	public ComparableBCMessageTask(Runnable runnable, T result, PriorityLevel jobPriority, Long jobCreationTime, Integer procedureIndex,
			BCMessageStatus messageStatus, Long messageCreationTime) {
		super(runnable, result);
		this.jobPriority = jobPriority;
		this.jobCreationTime = jobCreationTime;
		this.procedureIndex = procedureIndex;
		this.messageStatus = messageStatus;
		this.messageCreationTime = messageCreationTime;
	}


	@Override
	public int compareTo(ComparableBCMessageTask<T> o) {
		if (jobPriority == o.jobPriority) {
			if (jobCreationTime == o.jobCreationTime) {
				if (procedureIndex == o.procedureIndex) {
					if (messageStatus == o.messageStatus) {
						if (messageCreationTime == o.messageCreationTime) {
							return 0;
						} else {
							return -messageCreationTime.compareTo(o.messageCreationTime);
						}
					} else {
						return messageStatus.compareTo(o.messageStatus);
					}
				} else {
					return -procedureIndex.compareTo(o.procedureIndex);
				}
			} else {
				return -jobCreationTime.compareTo(o.jobCreationTime);
			}
		} else {
			return jobPriority.compareTo(o.jobPriority);
		}
	}
}

// public ComparableFutureTask(Callable<T> callable, int priority) {
// super(callable);
// }