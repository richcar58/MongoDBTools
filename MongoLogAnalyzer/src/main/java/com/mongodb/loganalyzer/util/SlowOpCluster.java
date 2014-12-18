package com.mongodb.loganalyzer.util;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Range;

public class SlowOpCluster
{
  /* *************************************************************************** */
  /*                                  Fields                                     */
  /* *************************************************************************** */
  private long maxDuration;
  private Range<Long> range;
  private List<SlowOp> slowOps = new ArrayList<SlowOp>();
  
  /* *************************************************************************** */
  /*                             Constructors                                    */
  /* *************************************************************************** */
  /** Initialize the slow op list with the first operation and use its
   * range as the initial range for the cluster.
   * 
   * @param slowOp the initial operation in the cluster.
   */
  public SlowOpCluster(SlowOp slowOp)
  {
    slowOps.add(slowOp);
    this.range = slowOp.getRange();
    maxDuration = slowOp.getDuration();
  }
  
  /* *************************************************************************** */
  /*                               Public Methods                                */
  /* *************************************************************************** */
  /** This method conditionally add a slow operation to the cluster if that 
   * operation overlaps with the cluster's range.  If there is overlap, the 
   * operation is added to the cluster's list and the cluster's range is adjusted
   * in either direction if necessary. 
   * 
   * @param slowOp the candidate operation that may be added to the cluster.
   * @return true if the operation was added, false if not.
   * 
   * */
  public boolean addOverlappingSlowOp(SlowOp slowOp)
  {
    Range<Long> newRange = slowOp.getRange();
    if (newRange.isConnected(range))
    {
      // Add operation cluster and adjust max duration if necessary.
      slowOps.add(slowOp);
      if (slowOp.getDuration() > maxDuration) maxDuration = slowOp.getDuration();
      
      // Initialize replacement range endpoints.
      Long start = range.lowerEndpoint();
      Long end   = range.upperEndpoint();
      
      // Widen range if necessary.
      if (newRange.lowerEndpoint() < start)
        start = newRange.lowerEndpoint();
      if (newRange.upperEndpoint() > end)
        end = newRange.upperEndpoint();
      range = Range.closed(start, end);
      
      // Let caller know that operation is now part of cluster.
      return true;
    }
    
    // No overlap between cluster and operation time periods.
    return false;
  }
  
  public List<SlowOp> getSlowOps()
  {
    return slowOps;
  }

  public void setSlowOps(List<SlowOp> slowOps)
  {
    this.slowOps = slowOps;
  }

  public long getMaxDuration()
  {
    return maxDuration;
  }

  public void setMaxDuration(long maxDuration)
  {
    this.maxDuration = maxDuration;
  }
  
  public Range<Long> getRange()
  {
    return range;
  }

  public void setRange(Range<Long> range)
  {
    this.range = range;
  }

  @Override
  public String toString(){return StringHelper.toString(this);}
}
