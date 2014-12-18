package com.mongodb.loganalyzer.util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;

public class AnalyzerCalc
{
  /* *************************************************************************** */
  /*                                  Fields                                     */
  /* *************************************************************************** */
  // Logger uses Logback.xml for configuration.
  private static final Logger LOG = LoggerFactory.getLogger(AnalyzerCalc.class);
  
  // Initial list of slow operations sort by ascending start time.
  private ArrayList<SlowOp> slowOps;
  private AnalyzerParms parms;
  
  // Output fields.
  private ArrayList<SlowOpCluster> clusters;
  private ArrayList<TimeLineItem>  timeline;

  /* *************************************************************************** */
  /*                             Constructors                                    */
  /* *************************************************************************** */
  public AnalyzerCalc(ArrayList<SlowOp> slowOps, AnalyzerParms parms)
  {
    // Catch problems early.
    if ((slowOps == null) || (parms == null))
      {
       String msg = "Null argument received by AnalyzerCalc constructor.";
       LOG.error(msg);
       throw new IllegalArgumentException(msg);
      }
    
    // Save input.
    this.slowOps = slowOps;
    this.parms = parms;
    
    // Initialize result fields.
    clusters = new ArrayList<SlowOpCluster>();
    timeline = new ArrayList<TimeLineItem>();
  }
  
  /* *************************************************************************** */
  /*                             Public Methods                                  */
  /* *************************************************************************** */
  /* --------------------------------------------------------------------------- */
  /* calculateClusters:                                                          */
  /* --------------------------------------------------------------------------- */
  public void calculate()
  {
    calculateClusters();
    calculateTimeLine();
  }
  
  /* --------------------------------------------------------------------------- */
  /* getClusters:                                                                */
  /* --------------------------------------------------------------------------- */
  public List<SlowOpCluster> getClusters(){return clusters;}

  /* --------------------------------------------------------------------------- */
  /* getTimeline:                                                                */
  /* --------------------------------------------------------------------------- */
  public List<TimeLineItem> getTimeline(){return timeline;}
  
  /* --------------------------------------------------------------------------- */
  /* toString:                                                                   */
  /* --------------------------------------------------------------------------- */
  @Override
  public String toString(){return StringHelper.toString(this);}
  
  /* *************************************************************************** */
  /*                              Private Methods                                */
  /* *************************************************************************** */
  /* --------------------------------------------------------------------------- */
  /* calculateClusters:                                                          */
  /* --------------------------------------------------------------------------- */
  private void calculateClusters()
  {
    // Maybe there's nothing to do.
    if (slowOps.isEmpty()) return;
    
    // Walk the list using indexing which is  
    // efficient since its an array list.
    for (int index = 0; index < slowOps.size();)
    {
      // Tracing
      if (LOG.isDebugEnabled()) LOG.debug("** index = " + index);
      
      // Initialize the cluster with the next slowOp.
      SlowOpCluster cluster = new SlowOpCluster(slowOps.get(index));
      clusters.add(cluster);
      
      // See if the next operation overlaps with the cluster's range.
      int clusterIndex = index + 1;
      for (; clusterIndex < slowOps.size(); clusterIndex++)
      {
        // Tracing
        if (LOG.isDebugEnabled()) LOG.debug("  ** clusterIndex = " + clusterIndex);
        
        // If the curOp
        if (!cluster.addOverlappingSlowOp(slowOps.get(clusterIndex)))
        {
          // Assign index the clusterIndex so that its operation
          // becomes the first operation in the next cluster.
          index = clusterIndex;
          break;
        }
      }
      
      // Check for end of outer loop condition.
      if (clusterIndex >= slowOps.size()) break;
    }
  }

  /* --------------------------------------------------------------------------- */
  /* calculateTimeLine:                                                          */
  /* --------------------------------------------------------------------------- */
  private void calculateTimeLine()
  {
    // Maybe there's nothing to do.
    if (slowOps.isEmpty()) return;
    
    // Increase the capacity of the timeline list.
    timeline.ensureCapacity(clusters.size() * 2);
    
    // Iterate through the clusters calculating the cluster
    // duration and the non-cluster duration (i.e., the time
    // inbetween clusters).
    long cumulativeTime = 0;
    Range<Long> lastRange = null;
    for (SlowOpCluster cluster : clusters)
    {
      // Calculate the time inbetween clusters if we're not at the first cluster.
      if (lastRange != null) 
      {
        // This works because clusters are ordered by lower endpoint 
        // and all overlapping ranges are coalesced.
        TimeLineItem nonCusterItem = new TimeLineItem();
        nonCusterItem.startMillis = cumulativeTime;
        nonCusterItem.duration = cluster.getRange().lowerEndpoint() - lastRange.upperEndpoint();
        cumulativeTime += nonCusterItem.duration;
        nonCusterItem.delayedOps = 0;
        timeline.add(nonCusterItem);
      }
      
      // Set up for next non-cluster duration.
      lastRange = cluster.getRange();
      
      // Add the cluster item to the timeline.
      TimeLineItem item = new TimeLineItem();
      item.startMillis = cumulativeTime;
      item.duration = cluster.getRange().upperEndpoint() - cluster.getRange().lowerEndpoint();
      cumulativeTime += item.duration;
      item.delayedOps = cluster.getSlowOps().size();
      timeline.add(item);
    }
    
  }
  
  /* *************************************************************************** */
  /*                              TimeLineItem Class                             */
  /* *************************************************************************** */
  public static final class TimeLineItem
  {
    public int  delayedOps;
    public long duration;
    public long startMillis;
    
    @Override
    public String toString(){return StringHelper.toString(this);}
  }

}
