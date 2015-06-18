package main.com.in.sparkrddtest;

/**
 * 统计项规则配置
 * 主要是为daily和activity配置提供统一的接口
 * @author Tang Zeliang
 * 
 */
public class ItemServer {
	/**
	 * 表示活动统计
	 */
	public static String ACTWORK = "activity";
	/**
	 * 表示日常统计
	 */
	public static String DAILYWORK = "daily";
	/**
	 * 表示推广统计
	 */
	public static String PROMOTEWORK = "promotion";
	/**
	 * 表示需要统计pv项
	 */
	public static String PVITEM = "pvitem";
	/**
	 * 表示需要统计uv项
	 */
	public static String UVITEM = "uvitem";
	/**
	 * 表示不需要进行统计
	 */
	public static String NEEDNOTCOUNT = "-1";

}
