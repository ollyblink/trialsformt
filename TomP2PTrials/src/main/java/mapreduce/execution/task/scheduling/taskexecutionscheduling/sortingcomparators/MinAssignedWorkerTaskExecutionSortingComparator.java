package mapreduce.execution.task.scheduling.taskexecutionscheduling.sortingcomparators;

import java.util.Comparator;

import mapreduce.execution.task.Task;

public class MinAssignedWorkerTaskExecutionSortingComparator implements Comparator<Task> {

	@Override
	public int compare(Task t1, Task t2) {
		if (!t1.isFinished() && t2.isFinished()) {
			return -1;
		} else if (t1.isFinished() && !t2.isFinished()) {
			return 1;
		} else {// if (!(t1.isFinished() && !t2.isFinished() || (t1.isFinished() && t2.isFinished())) {
			int t1NrFinished = t1.nrOfOutputDomains();
			int t2NrFinished = t2.nrOfOutputDomains();
			if (t1NrFinished > t2NrFinished) {
				return 1;
			} else if (t1NrFinished < t2NrFinished) {
				return -1;
			} else {
				if (t1.nrOfAssignedWorkers() > t2.nrOfAssignedWorkers()) {
					return 1;
				} else if (t2.nrOfAssignedWorkers() < t2.nrOfAssignedWorkers()) {
					return -1;
				} else {
					return 0;
				}
			}
		}
	}

	public static MinAssignedWorkerTaskExecutionSortingComparator create() {
		return new MinAssignedWorkerTaskExecutionSortingComparator();
	}
}
