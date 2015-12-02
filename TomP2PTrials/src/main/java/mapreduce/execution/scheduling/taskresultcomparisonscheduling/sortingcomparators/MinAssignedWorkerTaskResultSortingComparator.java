package mapreduce.execution.scheduling.taskresultcomparisonscheduling.sortingcomparators;

import java.util.Comparator;

import mapreduce.execution.jobtask.Task;

public class MinAssignedWorkerTaskResultSortingComparator implements Comparator<Task> {

	@Override
	public int compare(Task t1, Task t2) {
		if ((t1.finalDataLocation() != null && t2.finalDataLocation() != null)) {
			return 0;
		} else if (t1.finalDataLocation() == null && t2.finalDataLocation() == null) {
			if (t1.taskComparisonAssigned() && t2.taskComparisonAssigned()) {
				return 0;
			} else if (!t1.taskComparisonAssigned() && t2.taskComparisonAssigned()) {
				return 1;
			} else if (t1.taskComparisonAssigned() && !t2.taskComparisonAssigned()) {
				return -1;
			} else {
				return 0; // NOT POSSIBLE
			}
		} else if (t1.finalDataLocation() != null && t2.finalDataLocation() == null) {
			return -1;
		} else if (t1.finalDataLocation() == null && t2.finalDataLocation() != null) {
			return 1;
		} else {
			return 0; // NOT POSSIBLE
		}

	}

	private MinAssignedWorkerTaskResultSortingComparator() {

	}

	public static MinAssignedWorkerTaskResultSortingComparator newInstance() {
		return new MinAssignedWorkerTaskResultSortingComparator();
	}

}
