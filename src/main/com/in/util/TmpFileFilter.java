package main.com.in.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class TmpFileFilter implements PathFilter{

	@Override
	public boolean accept(Path arg0) {
		// TODO Auto-generated method stub
		if(arg0.getName().endsWith(".tmp")||arg0.getName().endsWith(".index")){
			return false;
		}else{
			return true;
		}
	}

}
