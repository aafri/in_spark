package main.com.in.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Aggregated key for LogStat
 * Map output will be partitioned by first part and sort by secondary part
 * @author tzl
 *
 */
public class LogAggrKey implements WritableComparable<LogAggrKey> {
	private Text _firstKey;	//first part of key
	private Text _secondKey;//second part of key
	
	/**
	 * Default constructor
	 */
	public LogAggrKey()
	{
		this._firstKey = new Text();
		this._secondKey = new Text();
	}
	
	/**
	 * Constructor with Text
	 * @param key1: first part of key
	 * @param key2: second part of key
	 */
	public LogAggrKey(Text key1, Text key2)
	{
		this._firstKey = key1;
		this._secondKey = key2;
	}
	
	/**
	 * Construct key with string
	 * @param key1: first key part
	 * @param key2: secondary key part
	 */
	public LogAggrKey(String key1, String key2)
	{
		this._firstKey = new Text(key1);
		this._secondKey = new Text(key2);
	}
	
	@Override
	/**
	 * Write the key to output stream
	 */
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		_firstKey.write(out);
		_secondKey.write(out);
	}

	@Override
	/**
	 * Read the key from input stream
	 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		_firstKey.readFields(in);
		_secondKey.readFields(in);
	}

	@Override
	/**
	 * Compare it with another key
	 */
	public int compareTo(LogAggrKey arg0) {
		// TODO Auto-generated method stub
		int cmp = this._firstKey.compareTo(arg0._firstKey);
		if (cmp == 0)
		{
			return this._secondKey.compareTo(arg0._secondKey);
		}
		else
		{
			return cmp;
		}
	}

	/**
	 * Get te first part of key
	 * @return first part of key
	 */
	public Text first()
	{
		return this._firstKey;
	}
}
