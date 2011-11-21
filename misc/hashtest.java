import java.math.BigDecimal;
import java.util.*;

public class hashtest
{

    private static final double ONE_MILLION = (double) 1000000;

	private static final int LIMIT = 1000000;

	private static final int kFNVOffset = (int) 2166136261l;
	private static final int kFNVPrime = 16777619;

    private static List<Long> times = new ArrayList<Long>();

    public static void recordNanoTime(long endTimeNanos, long startTimeNanos) {
        long time = endTimeNanos - startTimeNanos;

        if (time < 0) {
            // start time is +ve and end time is -ve due to wrapping of nanosecond timer.
            // in this instance add the difference between the start time and Long.MAX_VALUE
            // to the abs of the end time.
            long absEndTime = Math.abs(endTimeNanos);
            // if the value is already at Long.MIN_VALUE absing it will return a negatvie value (Long.MIN_VALUE).
            if (absEndTime < 0) {
                absEndTime = 0;
            }
            time = (Long.MAX_VALUE - startTimeNanos) + absEndTime;
        }

        times.add(time);

    }

	public static int fnvHashCode(final String str) {

        long startTimeNanos = System.nanoTime();

		int hash = kFNVOffset;
 		int len = str.length();

		for (int i = 0; i < len; i++) {
//			hash ^= (0x0000ffff & (int)str.charAt(i));
			hash ^= (int)str.charAt(i);
			hash *= kFNVPrime;
		}

//		return 0x00000000ffffffffl & hash;

        recordNanoTime(System.nanoTime(), startTimeNanos);

		return hash;
	}

	public static void main(String args[])
	{
		String str = "something";
		int hash = 0;

		System.out.println("Performing "+LIMIT+" hashes.");

        long startTime = System.currentTimeMillis();
		for (int i=0; i < LIMIT; i++) {
			hash = fnvHashCode(str + i);
		}
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime-startTime;
		System.out.println("[STR + I as key] Hash:" + hash + " " + (elapsedTime));

        generateStatsOutput(elapsedTime);

        times = new ArrayList<Long>();

        startTime = System.currentTimeMillis();
		for (int i=0; i < LIMIT; i++) {
			hash = fnvHashCode(str);
		}
        endTime = System.currentTimeMillis();
        elapsedTime = endTime-startTime;
		System.out.println("[STR as key] Hash:" + hash + " " + (elapsedTime));

        generateStatsOutput(elapsedTime);

	}

    public static void generateStatsOutput(long elapsedTime) {
        StringBuffer result = new StringBuffer();
        try {

            Long[] timesArray = times.toArray(new Long[times.size()]);

            int count = timesArray.length;

            Arrays.sort(timesArray);

            int minIndex = 0;
            int maxIndex = count - 1;
            if (maxIndex < 0) {
                maxIndex = 0;
            }

            int percentile50Index = (int) (maxIndex * 0.50);
            if (percentile50Index > maxIndex) {
                percentile50Index = maxIndex;
            }

            Long median;

            if (percentile50Index != maxIndex && percentile50Index * 2 > maxIndex) {
                median = (timesArray[percentile50Index] + timesArray[percentile50Index - 1]) / 2;
            } else if (percentile50Index != 0 && percentile50Index * 2 < maxIndex) {
                median = (timesArray[percentile50Index] + timesArray[percentile50Index + 1]) / 2;
            } else {
                median = timesArray[percentile50Index];
            }

            int percentile75Index = (int) (maxIndex * 0.75);
            if (percentile75Index >= maxIndex) {
                percentile75Index = maxIndex;
            }

            int percentile90Index = (int) (maxIndex * 0.90);
            if (percentile90Index >= maxIndex) {
                percentile90Index = maxIndex;
            }

            int percentile95Index = (int) (maxIndex * 0.95);
            if (percentile95Index >= maxIndex) {
                percentile95Index = maxIndex;
            }

            int percentile99Index = (int) (maxIndex * 0.99);
            if (percentile99Index >= maxIndex) {
                percentile99Index = maxIndex;
            }

            int i = 0;
            Long totalTime = 0L;
            Map<Long, Integer> modeMap = new HashMap<Long, Integer>();
            Long modeKey = null;
            Integer modeCount = null;
            for (; i < count; i++) {
                // calc total for average
                totalTime += timesArray[i];
                // what is the mode
                Integer modeVal = modeMap.get(timesArray[i]);
                if (modeVal == null) {
                    modeVal = 0;
                }
                modeVal += 1;
                if (modeCount == null || modeVal > modeCount) {
                    modeKey = timesArray[i];
                    modeCount = modeVal;
                }
                modeMap.put(timesArray[i], modeVal);
            }

            BigDecimal averageTime;
            BigDecimal throughput;

            try {
                averageTime = new BigDecimal((double) (totalTime / count)).setScale(0, BigDecimal.ROUND_HALF_UP);
                throughput = new BigDecimal((double) (1000F * count / elapsedTime)).setScale(3, BigDecimal.ROUND_HALF_UP);
            } catch (NumberFormatException e) {
                averageTime = BigDecimal.ZERO;
                throughput = BigDecimal.ZERO;
            }

            Long min = timesArray[minIndex];
            Long max = timesArray[maxIndex];
            Long percentile50 = timesArray[percentile50Index];
            Long percentile75 = timesArray[percentile75Index];
            Long percentile90 = timesArray[percentile90Index];
            Long percentile95 = timesArray[percentile95Index];
            Long percentile99 = timesArray[percentile99Index];

            result.append("[ ").append(elapsedTime).append(" ms ] ").append(count).append(" @ ").append(throughput).append(" /sec")
                    .append(", Avg: ").append(Double.toString(averageTime.doubleValue() / ONE_MILLION)).append(" ms")
                    .append(", Min: ").append(Double.toString(min / ONE_MILLION)).append(" ms")
                    .append(", Max: ").append(Double.toString(max / ONE_MILLION)).append(" ms")
                    .append(", Median: ").append(Double.toString(median / ONE_MILLION)).append(" ms");

            result.append(", Mode: ").append(Double.toString(modeKey / ONE_MILLION)).append(" ms @ ").append(modeCount).append(" ");

            result.append(", 50%: ").append(Double.toString(percentile50 / ONE_MILLION)).append(" ms")
                    .append(", 75%: ").append(Double.toString(percentile75 / ONE_MILLION)).append(" ms")
                    .append(", 90%: ").append(Double.toString(percentile90 / ONE_MILLION)).append(" ms")
                    .append(", 95%: ").append(Double.toString(percentile95 / ONE_MILLION)).append(" ms")
                    .append(", 99%: ").append(Double.toString(percentile99 / ONE_MILLION)).append(" ms");


        } catch (Exception e) {
            e.printStackTrace();
        }

        if (result.length() > 0) {
            System.out.println(result.toString());
        }
    }


}
