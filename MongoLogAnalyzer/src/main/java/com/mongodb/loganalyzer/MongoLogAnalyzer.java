package com.mongodb.loganalyzer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.mongodb.loganalyzer.util.AnalyzerCalc;
import com.mongodb.loganalyzer.util.AnalyzerParms;
import com.mongodb.loganalyzer.util.AnalyzerReport;
import com.mongodb.loganalyzer.util.SlowOp;
import com.mongodb.loganalyzer.util.SlowOpComparator;

public class MongoLogAnalyzer
{
  /* *************************************************************************** */
  /*                                  Fields                                     */
  /* *************************************************************************** */
  // Logger uses Logback.xml for configuration.
  private static final Logger LOG = LoggerFactory.getLogger(MongoLogAnalyzer.class);
  
  // This is the formatter that uses the old Java Date facility to parse the 
  // default mongod log date format.  See the --logTimestampFormat command line
  // parameter for details.  This formatter parses iso8601-local such as: 
  // 1969-12-31T19:00:00.000+0500
  private static final String formatIsoLocal = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final SimpleDateFormat isoLocalFormat = new SimpleDateFormat(formatIsoLocal);

  // The input parameters.
  private AnalyzerParms parms;
  
  // Statistics.
  private int numLogRecordsRead;
  
  // Calculation object.
  private AnalyzerCalc calc;

  /* *************************************************************************** */
  /*                             Constructors                                    */
  /* *************************************************************************** */
  /** This object must be initialized with non-null parameters.
   * 
   * @param parms non-null input parameters object. */
  public MongoLogAnalyzer(AnalyzerParms parms)
  {
    if (parms == null) throw new IllegalArgumentException("Parameters cannot be null.");
    this.parms = parms;
  }

  /* *************************************************************************** */
  /*                             Public Methods                                  */
  /* *************************************************************************** */
  /* --------------------------------------------------------------------------- */
  /* main:                                                                       */
  /* --------------------------------------------------------------------------- */
  /** Run from command-line.
   * 
   * @param args required and optional parms
   * @throws Exception if something goes wrong */
  public static void main(String[] args) throws Exception
  {
    // Tracing.
    LOG.info("Starting MongoLogAnalyzer.");
    
    // Run log analysis and print results.
    AnalyzerParms parms = initParms(args);
    MongoLogAnalyzer analyzer = new MongoLogAnalyzer(parms);
    analyzer.analyze();
    analyzer.writeResults();
    
    // Tracing.
    LOG.info("Stopping MongoLogAnalyzer.");
  }

  /* --------------------------------------------------------------------------- */
  /* analyze:                                                                    */
  /* --------------------------------------------------------------------------- */
  public void analyze() throws IOException
  {
    // Get all lines in log file and record the number read.
    List<String> lines = readLines();
    numLogRecordsRead = lines.size();

    // Collect all slow operations.
    ArrayList<SlowOp> slowOps = new ArrayList<SlowOp>();
    for (String line : lines)
    {
      SlowOp slowOp = getSlowOp(line);
      if (slowOp != null) slowOps.add(slowOp);
    }
    
    // Allow memory to be freed.
    lines = null;
    
    // Sort the slow ops by start time.
    slowOps.sort(new SlowOpComparator());
    
    // Calculate the slow op clusters.
    calc = new AnalyzerCalc(slowOps, parms);
    calc.calculate();
  }

  /* *************************************************************************** */
  /*                              Private Methods                                */
  /* *************************************************************************** */
  /* --------------------------------------------------------------------------- */
  /* initParms:                                                                  */
  /* --------------------------------------------------------------------------- */
  /** Parse command-line arguments as defined in SCMigrateParms.
   * 
   * @param args command-line arguments
   * @return the parsed input in a SCMigrateParms object. */
  private static AnalyzerParms initParms(String[] args)
  {
    AnalyzerParms parms = new AnalyzerParms(args);
    return parms;
  }

  /* --------------------------------------------------------------------------- */
  /* readLines:                                                                  */
  /* --------------------------------------------------------------------------- */
  /** Read the mongod log file into a list of records. */
  private List<String> readLines() throws IOException
  {
    // Use plain old, in-order processing of all lines in file.
    List<String> lines = null;
    try
    {
      lines = Files.readLines(parms.mongoLog, Charset.defaultCharset());
    }
    catch (Exception e)
    {
      String msg = "Unable to read lines from file " + parms.mongoLog.getAbsolutePath() + ".";
      LOG.error(msg, e);
      throw e;
    }
    
    return lines;
  }
  
  /* --------------------------------------------------------------------------- */
  /* getSlowOps:                                                                 */
  /* --------------------------------------------------------------------------- */
  private SlowOp getSlowOp(String line)
  {
    // Normalize input string.
    line = line.trim();
    
    // --------- Get Timestamp Component
    // Get the first word in the line.  For speed, that the only whitespace used 
    // are spaces.  This assumption may not hold in the future.
    int firstWsIndex = line.indexOf(" ");  
    if (firstWsIndex < 0) return null;     // not a line we care about
    String ts = line.substring(0, firstWsIndex);
    
    // As usual, dates are a real pain.  The iso standard allow for differences in
    // the way the timezone offset is written.  Java chose the +00:00 format for
    // its ISO_OFFSET_DATE_TIME, mongod chose the +0000 format.  To workaround the
    // incompatibility, we use the old Java Date facility.
    //
    // Note that monogd can be started by specifying any of 3 date formats:
    //
    //  --logTimestampFormat=(ctime|iso8601-utc|iso8601-local)
    //     ctime:         Wed Dec 31 19:00:00.000
    //     iso8601-utc:   1970-01-01T00:00:00.000Z
    //     iso8601-local: 1969-12-31T19:00:00.000+0500
    //
    // This program supports the iso formats be not the ctime format since it
    // does not specify year.  This imprecision makes working with logs that
    // span multiple years difficult to deal with.

    // The parsed date.
    Instant instant = null;
    
    // 1st Try:  iso8601-local is the default date.
    // Example:  2011-12-03T10:15:30.123+0100
    try
    {
      // Use old parser to 
      Date date = isoLocalFormat.parse(ts);
      instant = date.toInstant();
    }
    catch (Exception e){} // It's ok to fail here.
    
    // 2nd Try:  iso8601-utc
    // Example:  2011-12-03T09:15:30.123Z
    try {instant = Instant.parse(ts);} 
     catch (DateTimeParseException e){}  // not a record we're going to parse
    
    // Did we succeed?
    if (instant == null) return null;
    
    // --------- Get Duration Component
    // Again, we make assumptions about the types of whitespace in the log.
    // We know there's at least one space in the string from the above search
    // and because of the trim() call, we know there's at least one non-ws 
    // character after it.
    int lastWsIndex = line.lastIndexOf(" ");
    String duration = line.substring(lastWsIndex+1);
    if (!duration.endsWith("ms")) return null;
    duration = duration.substring(0, duration.length() - 2); // chop off the "ms"
    
    // Attempt to convert the duration to a long.
    long dur = -1;
    try {dur = Long.parseLong(duration);}
     catch (Exception e){return null;}  // not what we expected
      
    // Return the slow operation.
    return new SlowOp(instant, dur);
  }
  
  /* --------------------------------------------------------------------------- */
  /* writeResults:                                                               */
  /* --------------------------------------------------------------------------- */
  private void writeResults()
  {
    // Write results.
    System.out.println(numLogRecordsRead + " records read from " + parms.mongoLog + ".");
    AnalyzerReport report = new AnalyzerReport(calc, parms);
    System.out.println(report.getClusterSummaryReport(false));
    System.out.println(report.getTimeLineReport());
  }
}
