package utils;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class TupleComparator implements Comparator<Tuple2<String, String>>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
		return o1._1().compareTo(o2._1());
	}
}
