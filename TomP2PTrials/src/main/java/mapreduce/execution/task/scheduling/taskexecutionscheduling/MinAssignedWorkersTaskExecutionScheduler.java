package mapreduce.execution.task.scheduling.taskexecutionscheduling;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.task.Task;
import mapreduce.execution.task.Task2;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.sortingcomparators.MinAssignedWorkerTaskExecutionSortingComparator;

/**
 * Scheduler simply orders the tasks such that those tasks with least currently assigned peers to execute it are favoured. Tasks with less different
 * executors are favoured over such that already have a larger diversity. Lastly, Tasks with more <code>JobStatus.EXECUTING_TASK</code> are also
 * favoured over tasks with more <code>JobStatus.FINISHED_TASK</code>. If all tasks are finished, null is returned.
 * 
 * @author ozihler
 *
 */
public class MinAssignedWorkersTaskExecutionScheduler extends AbstractTaskExecutionScheduler {
	 private static Logger logger = LoggerFactory.getLogger(MinAssignedWorkersTaskExecutionScheduler.class);
	// private boolean isFirstTaskRandom;
	private MinAssignedWorkerTaskExecutionSortingComparator comparator;
	private RandomTaskExecutionScheduler randomTaskScheduler;

	private MinAssignedWorkersTaskExecutionScheduler() {
		this.comparator = MinAssignedWorkerTaskExecutionSortingComparator.newInstance();
	}

	public static MinAssignedWorkersTaskExecutionScheduler newInstance() {
		return new MinAssignedWorkersTaskExecutionScheduler().randomizeFirstTask(true);
	}

	public MinAssignedWorkersTaskExecutionScheduler randomizeFirstTask(boolean isFirstTaskRandom) {
		this.randomTaskScheduler = (isFirstTaskRandom ? RandomTaskExecutionScheduler.newInstance() : null);
		return this;
	}

	@Override
	protected Task2 scheduleNonNull(List<Task2> tasksToSchedule) {
		logger.info("Tasks to schedule");
//		for(Task2 task: tasksToSchedule){
//			logger.info(task.id() + ", ExecutingPeers stati: "+task.executingPeers().values());
//		}
		Task2 assignedTask = null;
		if (!allTasksAreFinished(tasksToSchedule)) {
			if (randomTaskScheduler != null && noTaskAssignedYet(tasksToSchedule)) {
				assignedTask = randomTaskScheduler.scheduleNonNull(tasksToSchedule);
				logger.info("random task assigned" + assignedTask);
			} else {
				Collections.sort(tasksToSchedule, this.comparator);
				Task2 task = tasksToSchedule.get(0);
				if (!task.isFinished() || task.isActive()) {
					assignedTask = task;
				} 
				logger.info("compared task assigned" + assignedTask);
			}
		} else {
			logger.info("finished procedure " + procedureInformation);
			// all tasks finished... set procedure to be finished
//			this.procedureInformation.isFinished(true);
		}
		return assignedTask;
	}

}
