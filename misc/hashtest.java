class hashtest 
{

	private static final int LIMIT = 50000000;

	private static final int kFNVOffset = (int) 2166136261l;
	private static final int kFNVPrime = 16777619;

	public static int fnvHashCode(final String str) {
		int hash = kFNVOffset;
 		int len = str.length();

		for (int i = 0; i < len; i++) {
//			hash ^= (0x0000ffff & (int)str.charAt(i));
			hash ^= (int)str.charAt(i);
			hash *= kFNVPrime;
		}

//		return 0x00000000ffffffffl & hash;
		return hash;
	}

	public static void main(String args[])
	{
		String str = "something";
		int hash = 0;

		System.out.println("Performing "+LIMIT+" hashes.");

		for (int i=0; i < LIMIT; i++) {
			hash = fnvHashCode(str);
		}

		System.out.println("Hash:" + hash);
	}

}
