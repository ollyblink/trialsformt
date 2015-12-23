package mapreduce.execution.job;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.Tasks;
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



	public Multimap<Task, Tuple<PeerAddress, Integer>> taskDataToRemove(List<Task> tasks, int currentProcedureIndex) {
		Multimap<Task, Tuple<PeerAddress, Integer>> toRemove = ArrayListMultimap.create();

		for (Task task : tasks) {
			toRemove.putAll(task, task.dataToRemove());
		}
		return toRemove;
	}

//	public boolean isFinishedFor(IMapReduceProcedure procedure) {
//		for (ProcedureTaskTuple tuple : procedures) {
//			if (tuple.procedure().equals(procedure)) {
//				return tuple.isFinished();
//			}
//		}
//		return false;
//	}

//	public void isFinishedFor(IMapReduceProcedure procedure, boolean isFinished) {
//		for (ProcedureTaskTuple tuple : procedures) {
//			if (tuple.procedure().equals(procedure)) {
//				tuple.isFinished(isFinished);
//				break;
//			}
//		}
//	}
}
