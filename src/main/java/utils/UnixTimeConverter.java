package utils;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;

public class UnixTimeConverter {
	public static Integer time2Year(long unixSeconds){
		Date date = new Date(unixSeconds*1000L); 
		SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy"); 
		String formattedDate = sdf.format(date);
		return Integer.parseInt(formattedDate);
	}
	
	public static IntWritable time2Year(String unixSeconds){
		Date date = new Date(Long.parseLong(unixSeconds)*1000L); 
		SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy"); 
		String formattedDate = sdf.format(date);
		IntWritable result = new IntWritable();
		result.set(Integer.parseInt(formattedDate));
		return result;
	}
}
