package mapreduce.execution.task.scheduling.taskexecutionscheduling.sortingcomparators;

import java.util.Comparator;

import mapreduce.execution.task.Task;
import mapreduce.execution.task.Tasks;

public class MinAssignedWorkerTaskExecutionSortingComparator implements Comparator<Task> {

	@Override
	public int compare(Task t1, Task t2) {
		 
		if (t1.isFinished() && t2.isFinished()) {
			return 0;
		} else if (!t1.isFinished() && t2.isFinished()) {
			return -1;
		} else if (t1.isFinished() && !t2.isFinished()) {
			return 1;
		} else {
			int t1Finished = Tasks.totalNumberOfFinishedExecutions(t1);
			int t2Finished = Tasks.totalNumberOfFinishedExecutions(t2);
			if (t1Finished > t2Finished) {
				return 1;
			} else if (t1Finished < t2Finished) {
				return -1;
			} else {
				int t1differentFinished = Tasks.numberOfDifferentPeersExecutingOrFinishedTask(t1);
				int t2differentFinished = Tasks.numberOfDifferentPeersExecutingOrFinishedTask(t2);
				if (t1differentFinished > t2differentFinished) {
					return 1;
				} else if (t1differentFinished < t2differentFinished) {
					return -1;
				} else {
					int t1Executing = Tasks.totalNumberOfCurrentExecutions(t1);
					int t2Executing = Tasks.totalNumberOfCurrentExecutions(t2);
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
	}

	public static MinAssignedWorkerTaskExecutionSortingComparator newInstance() {
		return new MinAssignedWorkerTaskExecutionSortingComparator();
	}
}
