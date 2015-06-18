package main.com.in.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Aggregated output value of mapper program
 * first part is a text: target
 * second part is a long value: count
 * @author tzl
 *
 */
public class LogAggrValue implements Writable {
	private Text _target;			//target
	private LongWritable _count;	//count

	/**
	 * Constructor 
	 */
	public LogAggrValue()
	{
		this._target = new Text();
		this._count = new LongWritable();
	}
	
	/**
	 * Constructor of text and longwritable
	 * @param text
	 * @param cnt
	 */
	public LogAggrValue(Text text, LongWritable cnt)
	{
		_target = text;
		_count = cnt;
	}
	
	/**
	 * Constructor of string and long
	 * @param text
	 * @param cnt
	 */
	public LogAggrValue(String text, long cnt)
	{
		_target = new Text(text);
		_count = new LongWritable(cnt);
	}
	
	/**
	 * Write it to output stream
	 */
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		_target.write(out);
		_count.write(out);
	}

	/**
	 * read it from input stream
	 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		_target.readFields(in);
		_count.readFields(in);
	}
	
	/**
	 * get the target from composited value
	 * @return: the target (string part)
	 */
	public Text getTarget() {
		return _target;
	}
	
	/**
	 * Set the target value
	 * @param _target
	 */
	public void setTarget(Text _target) {
		this._target = _target;
	}
	
	/**
	 * Get the count part
	 * @return count
	 */
	public LongWritable getCount() {
		return _count;
	}
	
	/**
	 * Set the count of value
	 * @param _count
	 */
	public void setCount(LongWritable _count) {
		this._count = _count;
	}
}
