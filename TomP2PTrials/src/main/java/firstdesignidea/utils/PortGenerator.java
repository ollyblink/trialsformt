package firstdesignidea.utils;
import java.util.Random;

public class PortGenerator {

	private static Random random = new Random();

	public static int generatePort() {
		return random.nextInt(1000) + 4000;
	}

}
