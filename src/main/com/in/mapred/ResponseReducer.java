package main.com.in.mapred;

import java.io.IOException;
import java.text.DecimalFormat;

import com.in.mapred.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Response Reducer to addup the count and response count of target
 * 
 * @author tzl
 *
 */
public class ResponseReducer extends Reducer<Text, com.in.mapred.ResponseValue, Text, Text> {

	/**
	 * Reduce the data
	 */
	public void reduce(Text key, Iterable<com.in.mapred.ResponseValue> values, Context context)
			throws IOException, InterruptedException {

		long totalCount = 0L;
		Double totalReps = (double) 0L;
		long wrongCount = 0L;
		Double wrongReps = (double) 0L;

		DecimalFormat df2 = new DecimalFormat("#.###");

		for (com.in.mapred.ResponseValue value : values) {
			// read the target and cnt from value
			Double cnt = value.getCount().get();// response
			String state = value.getTarget().toString();

			// add up the count
			totalCount++;
			totalReps += cnt;

			// wrong state stat
			if (state.startsWith("4") || state.startsWith("5")) {
				wrongCount++;
				wrongReps += cnt;
			}

		}
		// write out the result at the end of one group, data grouping is done
		// by LogGroupComparator which ensure the
		// data of same target is in one group  /app/notice/ret_23	100	50	2	5	16	3.2
		if (wrongCount == 0l) {
			context.write(key,
					new Text(totalCount + "\t" + df2.format(totalReps) + "\t"
							+ df2.format((totalReps / totalCount)) + "\t"
							+ wrongCount + "\t" + df2.format(wrongReps) + "\t"
							+ wrongCount));
		} else {
			context.write(key,
					new Text(totalCount + "\t" + df2.format(totalReps) + "\t"
							+ df2.format((totalReps / totalCount)) + "\t"
							+ wrongCount + "\t" + df2.format(wrongReps) + "\t"
							+ df2.format((wrongReps / wrongCount))));
		}
	}
}
