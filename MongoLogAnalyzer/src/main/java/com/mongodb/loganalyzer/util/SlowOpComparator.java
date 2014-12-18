package com.mongodb.loganalyzer.util;

import java.util.Comparator;

public class SlowOpComparator
implements Comparator<SlowOp>
{
  @Override
  public int compare(SlowOp o1, SlowOp o2)
  {
    // This comparator orders objects from low to high start times. 
    if (o1.getRange().lowerEndpoint() < o2.getRange().lowerEndpoint()) return -1;
    if (o2.getRange().lowerEndpoint() < o1.getRange().lowerEndpoint()) return 1;
    return 0;
  }
}
