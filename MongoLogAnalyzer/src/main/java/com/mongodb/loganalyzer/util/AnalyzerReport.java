package com.mongodb.loganalyzer.util;

import java.util.ArrayList;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.loganalyzer.util.AnalyzerCalc.TimeLineItem;

public class AnalyzerReport
{
  /* *************************************************************************** */
  /*                                  Fields                                     */
  /* *************************************************************************** */
  // Logger uses Logback.xml for configuration.
  private static final Logger LOG = LoggerFactory.getLogger(AnalyzerReport.class);
  
  // Fields initialized on construction.
  private AnalyzerCalc  calc;
  private AnalyzerParms parms;

  /* *************************************************************************** */
  /*                             Constructors                                    */
  /* *************************************************************************** */
  public AnalyzerReport(AnalyzerCalc calc, AnalyzerParms parms)
  {
    // Catch problems early.
    if ((calc == null) || (parms == null))
      {
       String msg = "Null argument received by AnalyzerReport constructor.";
       LOG.error(msg);
       throw new IllegalArgumentException(msg);
      }
    
    this.calc = calc;
    this.parms = parms;
  }
  
  /* *************************************************************************** */
  /*                             Public Methods                                  */
  /* *************************************************************************** */
  /* --------------------------------------------------------------------------- */
  /* getClusterSummaryReport:                                                    */
  /* --------------------------------------------------------------------------- */
  public String getClusterSummaryReport(boolean details)
  {
    StringBuilder buf = new StringBuilder(8096);
    buf.append("======================== Cluster Summary Report =======================\n");
    buf.append("Total clusters detected: ");
    buf.append(calc.getClusters().size());
    
    // Cut off processing here to when there are no clusters.
    if (calc.getClusters().size() == 0) return buf.toString();
    
    // Info message.
    buf.append("\nTimes in milliseconds unless otherwise indicated.");
    buf.append("\n\n");
    
    int cnt = 0;
    for (SlowOpCluster cluster : calc.getClusters())
    {
      buf.append("--------------------------------- Cluster ");
      buf.append(++cnt);
      buf.append("\n");
      buf.append("SlowOps = ");
      buf.append(cluster.getSlowOps().size());
      buf.append(", max operation time = ");
      buf.append(cluster.getMaxDuration());
      buf.append(", cluster duration = ");
      buf.append(cluster.getRange().upperEndpoint() - cluster.getRange().lowerEndpoint());
      buf.append("\n");
      if (details) buf.append(cluster.toString());
      
    }
    buf.append("====================== End Cluster Summary Report =====================\n");
    
    return buf.toString();
  }

  /* --------------------------------------------------------------------------- */
  /* getTimeLineReport:                                                          */
  /* --------------------------------------------------------------------------- */
  public String getTimeLineReport()
  {
    // Initial output.
    StringBuilder buf = new StringBuilder(2048);
    buf.append("======================== TimeLine Report ==============================\n");
    buf.append("Total clusters detected: ");
    buf.append(calc.getClusters().size());
    
    // Cut off processing here to when there are no clusters.
    if (calc.getClusters().size() == 0) return buf.toString();
    
    // Info message.
    buf.append("\nTimes in milliseconds unless otherwise indicated.");
    buf.append("\n\n");
    
    // Initialize counters.
    int cnt = 0;
    long totalClusterTime = 0;
    long totalNonClusterTime = 0;
    long maxClusterTime = 0;
    long minClusterTime = Long.MAX_VALUE;
    long maxNonClusterTime = 0;
    long minNonClusterTime = Long.MAX_VALUE;
    ArrayList<Double> clusterTimes = new ArrayList<Double>(calc.getClusters().size());
    ArrayList<Double> nonClusterTimes = new ArrayList<Double>(calc.getClusters().size());
    
    // Process each time line item.
    for (TimeLineItem item : calc.getTimeline())
    {
      // Output start time.
      buf.append("[");
      buf.append(item.startMillis);
      buf.append("] ");
      
      // Output duration for clusters and non-clusters; accumulate counters.
      String type = (item.delayedOps == 0) ? " Execution time" : "Cluster time";
      buf.append(type);
      if (item.delayedOps == 0)
      {
        totalNonClusterTime += item.duration;
        if (item.duration > maxNonClusterTime) maxNonClusterTime = item.duration;
        if (item.duration < minNonClusterTime) minNonClusterTime = item.duration;
        nonClusterTimes.add((double) item.duration);
      }
      else
      {
        buf.append(" ");
        buf.append(++cnt);
        totalClusterTime += item.duration;
        if (item.duration > maxClusterTime) maxClusterTime = item.duration;
        if (item.duration < minClusterTime) minClusterTime = item.duration;
        clusterTimes.add((double) item.duration);
      }
      buf.append(": ");
      buf.append(item.duration);
      buf.append("\n");
    }
    
    // Output cluster counters.
    buf.append("\nTotal cluster time: ");
    buf.append(totalClusterTime);
    buf.append("\n");
    buf.append("Maximum cluster time: ");
    buf.append(maxClusterTime);
    buf.append("\n");
    buf.append("Minumum cluster time: ");
    buf.append(minClusterTime);
    buf.append("\n");
    buf.append("Average cluster time: ");
    buf.append(totalClusterTime / calc.getClusters().size());  // non-zero denominator
    buf.append("\n");
    
    // Output standard deviation.
    double[] dblArray = new double[clusterTimes.size()];
    for (int i = 0; i < clusterTimes.size(); i++) dblArray[i] = clusterTimes.get(i);
    StandardDeviation sd = new StandardDeviation();
    double stddev = sd.evaluate(dblArray);
    buf.append("Cluster time standard deviation: ");
    buf.append((long)stddev);
    buf.append("\n");
    
    // Output non-cluster counters.
    buf.append("\nTotal non-cluster time: ");
    buf.append(totalNonClusterTime);
    buf.append("\n");
    buf.append("Maximum non-cluster time: ");
    buf.append(maxNonClusterTime);
    buf.append("\n");
    buf.append("Minumum non-cluster time: ");
    buf.append(minNonClusterTime);
    buf.append("\n");
    buf.append("Average non-cluster time: ");
    buf.append(totalNonClusterTime / calc.getClusters().size()); // non-zero denominator
    buf.append("\n");
    
    // Output standard deviation.
    dblArray = new double[nonClusterTimes.size()];
    for (int i = 0; i < nonClusterTimes.size(); i++) dblArray[i] = nonClusterTimes.get(i);
     sd = new StandardDeviation();
    stddev = sd.evaluate(dblArray);;
    buf.append("Non-cluster time standard deviation: ");
    buf.append((long)stddev);
    buf.append("\n");
    buf.append("====================== End TimeLine Report ============================\n");
    
    return buf.toString();
  }

}
