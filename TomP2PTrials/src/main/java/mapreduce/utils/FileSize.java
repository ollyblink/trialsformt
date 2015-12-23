package mapreduce.utils;

public enum FileSize {
	BYTE(1), KILO_BYTE(1024), TWO_KILO_BYTES(2 * KILO_BYTE.value()), FOUR_KILO_BYTES(4 * KILO_BYTE.value()), EIGHT_KILO_BYTES(
			8 * KILO_BYTE.value()), SIXTEEN_KILO_BYTES(16 * KILO_BYTE.value()), THIRTY_TWO_KILO_BYTES(32 * KILO_BYTE.value()), SIXTY_FOUR_KILO_BYTES(
					64 * KILO_BYTE.value()), MEGA_BYTE(KILO_BYTE.value() * KILO_BYTE.value()), TWO_MEGA_BYTES(2 * MEGA_BYTE.value()), FOUR_MEGA_BYTES(
							4 * MEGA_BYTE.value()), EIGHT_MEGA_BYTES(8 * MEGA_BYTE.value()), SIXTEEN_MEGA_BYTES(
									16 * MEGA_BYTE.value()), THIRTY_TWO_MEGA_BYTES(32 * MEGA_BYTE.value()), SIXTY_FOUR_MEGA_BYTES(
											64 * KILO_BYTE.value());

	private long value;

	FileSize(long value) {
		this.value = value;
	}

	public long value() {
		return value;
	}

	public static void main(String[] args) {
		System.out.println(BYTE.value());
		System.out.println(KILO_BYTE.value());
		System.out.println(TWO_KILO_BYTES.value());
		System.out.println(FOUR_KILO_BYTES.value());
		System.out.println(EIGHT_KILO_BYTES.value());
		System.out.println(SIXTEEN_KILO_BYTES.value());
		System.out.println(THIRTY_TWO_KILO_BYTES.value());
		System.out.println(SIXTY_FOUR_KILO_BYTES.value());
		System.out.println(MEGA_BYTE.value());
		System.out.println(TWO_MEGA_BYTES.value());
		System.out.println(FOUR_MEGA_BYTES.value());
		System.out.println(EIGHT_MEGA_BYTES.value());
		System.out.println(SIXTEEN_MEGA_BYTES.value());
		System.out.println(THIRTY_TWO_MEGA_BYTES.value());
		System.out.println(SIXTY_FOUR_MEGA_BYTES.value());
	}
}
