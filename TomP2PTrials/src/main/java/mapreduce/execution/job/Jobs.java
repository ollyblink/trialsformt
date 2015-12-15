package mapreduce.execution.job;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.ProcedureTaskTuple;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

/**
 * Holds methods to be used in connection to a job
 * 
 * @author Oliver
 *
 */
public enum Jobs {
	INSTANCE;

	public void updateTaskExecutionStatus(Job job, String taskId, TaskResult toUpdate) {
		if (job.tasks() != null) {
			List<Task> tasks = job.tasks();
			for (Task task : tasks) {
				if (task.id().equals(taskId)) {
					task.updateStati(toUpdate);
					break;
				}
			}
		}
	}

	public Multimap<Task, Tuple<PeerAddress, Integer>> taskDataToRemove(int currentProcedureIndex) {
		Multimap<Task, Tuple<PeerAddress, Integer>> toRemove = ArrayListMultimap.create();

		for (Task task : tasks) {
			toRemove.putAll(task, task.dataToRemove());
		}
		return toRemove;
	}

	public boolean isFinishedFor(IMapReduceProcedure procedure) {
		for (ProcedureTaskTuple tuple : procedures) {
			if (tuple.procedure().equals(procedure)) {
				return tuple.isFinished();
			}
		}
		return false;
	}

	public void isFinishedFor(IMapReduceProcedure procedure, boolean isFinished) {
		for (ProcedureTaskTuple tuple : procedures) {
			if (tuple.procedure().equals(procedure)) {
				tuple.isFinished(isFinished);
				break;
			}
		}
	}
}
