package mapreduce.execution.task.scheduling.taskexecutionscheduling.sortingcomparators;

import java.util.Comparator;

import mapreduce.execution.task.Task;

public class MinAssignedWorkerTaskExecutionSortingComparator implements Comparator<Task> {

	@Override
	public int compare(Task t1, Task t2) {
		int t1Finished = t1.totalNumberOfFinishedExecutions();
		int t2Finished = t2.totalNumberOfFinishedExecutions();
		if (t1Finished > t2Finished) {
			return 1;
		} else if (t1Finished < t2Finished) {
			return -1;
		} else {
			int t1differentFinished = t1.numberOfDifferentPeersExecutingTask();
			int t2differentFinished = t2.numberOfDifferentPeersExecutingTask();
			if (t1differentFinished > t2differentFinished) {
				return 1;
			} else if (t1differentFinished < t2differentFinished) {
				return -1;
			} else {
				int t1Executing = t1.totalNumberOfCurrentExecutions();
				int t2Executing = t2.totalNumberOfCurrentExecutions();
				if (t1Executing > t2Executing) {
					return 1;
				} else if (t1Executing < t2Executing) {
					return -1;
				} else {
					return 0;
				}
			}
		}
	}

	public static MinAssignedWorkerTaskExecutionSortingComparator newInstance() {
		return new MinAssignedWorkerTaskExecutionSortingComparator();
	}
}
