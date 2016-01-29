package mapreduce.engine.messageconsumers.updates;

import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;

public class ProcedureUpdate extends AbstractUpdate {
//	private static Logger logger = LoggerFactory.getLogger(ProcedureUpdate.class);

	private Job job;
	private JobCalculationMessageConsumer msgConsumer;

	public static ProcedureUpdate create(Job job, JobCalculationMessageConsumer msgConsumer) {
		return new ProcedureUpdate(job, msgConsumer);
	}

	private ProcedureUpdate(Job job, JobCalculationMessageConsumer msgConsumer) {
		this.job = job;
		this.msgConsumer = msgConsumer;
	}

	private ProcedureUpdate() {
	}

	@Override
	protected void internalUpdate(IDomain outputDomain, Procedure procedure) throws ClassCastException, NullPointerException {
		JobProcedureDomain outputJPD = (JobProcedureDomain) outputDomain;  
		procedure.addOutputDomain(outputJPD); 
		boolean procedureIsFinished = procedure.isFinished();
 		if (procedureIsFinished && procedure.dataInputDomain() != null) { 
			msgConsumer.cancelProcedureExecution(procedure.dataInputDomain().toString());
			JobProcedureDomain resultOutputDomain = procedure.resultOutputDomain();
			job.incrementProcedureIndex(); 
			job.currentProcedure().dataInputDomain(resultOutputDomain);  
			if (job.isFinished()) { 
				resultOutputDomain.isJobFinished(true);
			}
		} 
	}

} 