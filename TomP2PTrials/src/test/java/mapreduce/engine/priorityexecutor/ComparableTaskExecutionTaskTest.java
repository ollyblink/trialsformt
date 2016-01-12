package mapreduce.engine.priorityexecutor;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.task.Task;
import net.tomp2p.peers.Number160;

public class ComparableTaskExecutionTaskTest {

	@Test
	public void test() {
		List<Task> tasks = new ArrayList<>();
		for(int i = 0; i < 5;++i){
			tasks.add(Task.create(i+"").nrOfSameResultHash(3));
		} 

		tasks.get(0).addOutputDomain(ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, null).resultHash(Number160.ZERO));
		tasks.get(0).addOutputDomain(ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, null).resultHash(Number160.ZERO));
		tasks.get(0).addOutputDomain(ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, null).resultHash(Number160.ZERO));
		
		tasks.get(0).addOutputDomain(ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, null).resultHash(Number160.ZERO));
		tasks.get(0).addOutputDomain(ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, null).resultHash(Number160.ZERO));
		tasks.get(0).incrementActiveCount();

		tasks.get(0).addOutputDomain(ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, null).resultHash(Number160.ZERO));
		tasks.get(0).addOutputDomain(ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, null).resultHash(Number160.ONE));
		tasks.get(0).incrementActiveCount(); 
		tasks.get(0).incrementActiveCount();
		tasks.get(0).incrementActiveCount();
		
		tasks.get(0).addOutputDomain(ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, null).resultHash(Number160.ZERO)); 
		tasks.get(0).incrementActiveCount();
		tasks.get(0).incrementActiveCount();
		tasks.get(0).incrementActiveCount();

		tasks.get(0).addOutputDomain(ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, null).resultHash(Number160.ZERO)); 
		tasks.get(0).incrementActiveCount();

		tasks.get(0).incrementActiveCount();
		tasks.get(0).incrementActiveCount();
		tasks.get(0).incrementActiveCount(); 

		PriorityExecutor executor = PriorityExecutor.newFixedThreadPool(1);
		executor.submit(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub

			}

		}, tasks.get(0));
	}

}
