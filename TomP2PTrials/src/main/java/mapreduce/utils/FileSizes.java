package mapreduce.utils;

public enum FileSizes {
	BYTE(1), KILO_BYTE(1024), TWO_KILO_BYTE(2 * KILO_BYTE.value()), FOUR_KILO_BYTE(4 * KILO_BYTE.value()), EIGHT_KILO_BYTE(
			8 * KILO_BYTE.value()), SIXTEEN_KILO_BYTE(16 * KILO_BYTE.value()), THIRTY_TWO_KILO_BYTE(32 * KILO_BYTE.value()), SIXTY_FOUR_KILO_BYTE(
					64 * KILO_BYTE.value()), MEGA_BYTE(KILO_BYTE.value() * KILO_BYTE.value()), TWO_MEGA_BYTE(2 * MEGA_BYTE.value()), FOUR_MEGA_BYTE(
							4 * MEGA_BYTE.value()), EIGHT_MEGA_BYTE(8 * MEGA_BYTE.value()), SIXTEEN_MEGA_BYTE(
									16 * MEGA_BYTE.value()), THIRTY_TWO_MEGA_BYTE(32 * MEGA_BYTE.value()), SIXTY_FOUR_MEGA_BYTE(
											64 * KILO_BYTE.value());

	private long value;

	FileSizes(long value) {
		this.value = value;
	}

	public long value() {
		return value;
	} 

	public static void main(String[] args) { 
		System.out.println(BYTE.value());
		System.out.println(KILO_BYTE.value());
		System.out.println(TWO_KILO_BYTE.value());
		System.out.println(FOUR_KILO_BYTE.value());
		System.out.println(EIGHT_KILO_BYTE.value());
		System.out.println(SIXTEEN_KILO_BYTE.value());
		System.out.println(THIRTY_TWO_KILO_BYTE.value());
		System.out.println(SIXTY_FOUR_KILO_BYTE.value());
		System.out.println(MEGA_BYTE.value());
		System.out.println(TWO_MEGA_BYTE.value());
		System.out.println(FOUR_MEGA_BYTE.value());
		System.out.println(EIGHT_MEGA_BYTE.value());
		System.out.println(SIXTEEN_MEGA_BYTE.value());
		System.out.println(THIRTY_TWO_MEGA_BYTE.value());
		System.out.println(SIXTY_FOUR_MEGA_BYTE.value());
	}
}
