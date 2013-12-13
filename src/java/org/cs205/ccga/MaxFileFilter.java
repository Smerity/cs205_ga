package org.cs205.ccga;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class MaxFileFilter implements PathFilter {

  private static int count =  0;
  private static long max   = -1;
  private static String filter = "";
  
  public static void setFilter(String filter) {
    MaxFileFilter.filter = filter;
  }
  
  protected static void setMax(long newmax) {
    max = newmax;
  }

  @Override
  public boolean accept(Path path) {

    if (!path.getName().contains(filter)) {
      return false;
    }

    if (max < 0) {
      return true;
    }
    
    if (max < ++count) {
      return false;
    }
    
    return true;
  }
}