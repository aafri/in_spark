package com.in.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexpPathFilter implements PathFilter{

	private String regex;
	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		return path.getName().matches(regex);
	}
	
	public RegexpPathFilter(String regex){
		this.regex = regex;
	}
}
