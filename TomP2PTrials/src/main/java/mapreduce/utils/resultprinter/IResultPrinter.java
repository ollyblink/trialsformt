package mapreduce.utils.resultprinter;

import mapreduce.storage.IDHTConnectionProvider;

public interface IResultPrinter {

	void printResults(IDHTConnectionProvider dhtConnectionProvider, String domainString);

}
