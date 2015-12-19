package mapreduce.execution.task.taskdatacomposing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;

import org.junit.Test;

import mapreduce.utils.FileSize;

public class MaxFileSizeTaskDataComposerTest {

	@Test
	public void test() {
		MaxFileSizeTaskDataComposer instance = MaxFileSizeTaskDataComposer.create();
		instance.maxFileSize(FileSize.THIRTY_TWO_KILO_BYTES);
		instance.fileEncoding("UTF-8");
		instance.splitValue("\n");
		String value = null;
		String toAdd = "Allahu";
		long counter = 0;
		while ((value = instance.append(toAdd)) == null) {
			++counter;
		}

		String compare = "";
		for (int i = 0; i < counter; ++i) {
			compare += (toAdd + "\n");
		}
		assertEquals(compare, value);
		assertEquals(counter * (toAdd + "\n").getBytes(Charset.forName("UTF-8")).length, value.getBytes(Charset.forName("UTF-8")).length);
		assertTrue(FileSize.THIRTY_TWO_KILO_BYTES.value() >= value.getBytes(Charset.forName("UTF-8")).length);
	}

}
