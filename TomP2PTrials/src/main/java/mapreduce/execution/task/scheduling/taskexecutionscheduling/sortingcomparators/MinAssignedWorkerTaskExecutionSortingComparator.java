package mapreduce.execution.task.scheduling.taskexecutionscheduling.sortingcomparators;

import java.util.Comparator;

import mapreduce.execution.task.Task2;
import mapreduce.execution.task.Tasks;

public class MinAssignedWorkerTaskExecutionSortingComparator implements Comparator<Task2> {

	@Override
	public int compare(Task2 t1, Task2 t2) {

		if (t1.isFinished() && t2.isFinished()) {
			return 0;
		} else if (!t1.isFinished() && t2.isFinished()) {
			return -1;
		} else if (t1.isFinished() && !t2.isFinished()) {
			return 1;
		} else {
			int t1Finished = t1.outputDomains().size();
			int t2Finished = t1.outputDomains().size();
			if (t1Finished > t2Finished) {
				return 1;
			} else if (t1Finished < t2Finished) {
				return -1;
			} else {
				int t1differentFinished = t1.differentExecutors();
				int t2differentFinished = t2.differentExecutors();
				if (t1differentFinished > t2differentFinished) {
					return 1;
				} else if (t1differentFinished < t2differentFinished) {
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
