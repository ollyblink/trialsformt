import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.Tasks;
import mapreduce.utils.DomainProvider;

public class Taskupdate {
	
	executeJob(Job job){
		ProcedureInformation currentProcedure = job.currentProcedure();
		String dataLocationDomain = currentProcedure.dataLocationDomain();
		
		dht.getAll(DomainProvider.INSTANCE.PROCEDURE_KEYS, dataLocationDomain).addListener(new BaseFutureAdapter<FutureGet>(){
			operationComplete(future) {
				if(success){
					currentProcedure.taskSize(future.dataMap().size());
					for(Number640 nr: future.dataMap().size()){
						String key = ((String)future.dataMap().get(nr)); 
						currentProcedure.addTask(Task.create(key)); //tasks needs to be a set, not a list!!!!
					}
				}
			}
		});
	}
}
