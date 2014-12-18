package com.mongodb.loganalyzer.util;

import java.time.Instant;

import com.google.common.collect.Range;

/** Simple container class for slow operation record data. */
public class SlowOp 
{
  /* *************************************************************************** */
  /*                                  Fields                                     */
  /* *************************************************************************** */
  private Instant ts;         // Timestamp of log record
  private long duration;      // Operation ms from log record
  private Range<Long> range;  // Synthesized on construction

  /* *************************************************************************** */
  /*                               Constructors                                  */
  /* *************************************************************************** */
  /** Construct the range from timestamp and duration. 
  */
  public SlowOp(Instant ts, long duration)
  {
    this.ts = ts;
    this.duration = duration;
    range = Range.closed(ts.toEpochMilli() - duration, ts.toEpochMilli());
  }

  /* *************************************************************************** */
  /*                               Public Methods                                */
  /* *************************************************************************** */
  // See if two slow op ranges overlap.
  public boolean overlaps(SlowOp slowOp)
  {
    return range.isConnected(slowOp.range);
  }
  
  public Instant getTs()
  {
    return ts;
  }

  public void setTs(Instant ts)
  {
    this.ts = ts;
  }

  public long getDuration()
  {
    return duration;
  }

  public void setDuration(long duration)
  {
    this.duration = duration;
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