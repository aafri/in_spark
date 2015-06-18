package com.in.util;

/**
 * 计数器
 * 
 * @author Tang Zeliang
 * 
 */
public class Counter {
	// 默认的统计量
	private long cnt = 1;

	public Counter() {
	}

	public Counter(long cnt) {
		this.setCnt(cnt);
	}

	private void setCnt(long cnt) {
		this.cnt = cnt;
	}

	public long getCnt() {
		return cnt;
	}

	// 用于统计值自增
	public void increment() {
		this.setCnt(cnt + 1);
	}

	// 用于累加新的统计量
	public void plus(long newCnt) {
		// TODO Auto-generated method stub
		this.setCnt(cnt + newCnt);
	}
}
