package com.mongodb.loganalyzer.util;


import java.io.File;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/** We don't bother with getters and setters because this class is
 * not much more than a simple container for parsed input values.
 * 
 * @author RCardone
 */
public class AnalyzerParms
{
 /* *************************************************************************** */
 /*                                 Constants                                   */
 /* *************************************************************************** */
 private static final int DFT_SAMPLE_MS = 100;
 private static final int MIN_SAMPLE_MS = 10;
 
 private static final int DFT_PADTIME_MS = 0;
  
 /* *************************************************************************** */
 /*                                  Fields                                     */
 /* *************************************************************************** */
 @Option(name="-help", aliases={"--help"}, help=true, usage="Display this help")
 public boolean help;

// @Option(name="-s", required=false, aliases={"--sample_ms"},
//      metaVar="<ms>", usage="Sample time in milliseconds (> " + MIN_SAMPLE_MS + ")")
// public int sampleMs = DFT_SAMPLE_MS;
 
 @Argument(required=true, metaVar="<log file path>", usage="Input mongod log file")
 public String logfile;
 public File mongoLog; // Assign file object for caller's convenience

 /* *************************************************************************** */
 /*                               Constructors                                  */
 /* *************************************************************************** */
 /** Constructor used to parse command line arguments.
  * 
  * @param args command line arguments.
  */
 public AnalyzerParms(String[] args)
 {
  CmdLineParser parser = initParms(args);
  checkParms(parser);
 }
  
 /** Constructor used in non-command line cases. */
 public AnalyzerParms(){}
  
 /* *************************************************************************** */
 /*                               Public Methods                                */
 /* *************************************************************************** */
 /* --------------------------------------------------------------------------- */
 /* getParmString:                                                              */
 /* --------------------------------------------------------------------------- */
    /**
     * Return a content string for display.
     * @return String
     */
 public String getParmString()
 {
  String values = toString();
  int index = values.indexOf("[");
  if (index > 0) values = values.substring(index);
  return "--> MongoLogAnalyzer arguments:\n" + values;
 }

 /* *************************************************************************** */
 /*                               Private Methods                               */
 /* *************************************************************************** */
 /* --------------------------------------------------------------------------- */
 /* initParms:                                                                  */
 /* --------------------------------------------------------------------------- */
 /** Parse the input arguments. */
 private CmdLineParser initParms(String[] args)
 {
  CmdLineParser parser = new CmdLineParser(this);
  parser.getProperties().withUsageWidth(80);
  
  try {
     // parse the arguments.
     parser.parseArgument(args);
    }
   catch (CmdLineException e)
    {
     if (!help)
       {
        // Describe the problem.
        System.err.println("***** MongoLogAnalyzer aborted do to invalid input *****");
        System.err.println(e.getMessage());
        System.err.println();
     
        // Print the list of available options and exit.
        System.err.println("MongoLogAnalyzer [options...] mongodb-log-file\n");
        parser.printUsage(System.err);
        System.err.println();
        System.exit(1);
       }
    }
  
  // Display help and exit.
  if (help)
    {
     System.out.println(getUsageNotes());
     System.out.println("\nMongoLogAnalyzer [options...] mongodb-log-file\n");
     parser.printUsage(System.out);
     System.exit(0);
    }
  
  return parser;
 }
  
 /* --------------------------------------------------------------------------- */
 /* checkParms:                                                                 */
 /* --------------------------------------------------------------------------- */
 private void checkParms(CmdLineParser parser)
 {
   // Cumulative error flag.
   boolean failed = false;
   
   // Check that the file is readable.
   mongoLog = new File(logfile);
   if (!mongoLog.canRead())
   {
     failed = true;
     String msg = "Unable to find or read log file " + mongoLog.getAbsolutePath() + ".";
     System.err.println(msg);
   }
   
   // Did we pass all checks?
   if (failed)
   {
     System.err.println();
     System.err.println("MongoLogAnalyzer [options...] mongodb-log-file\n");
     parser.printUsage(System.err);
     System.err.println();
     System.exit(1);
   }
 }
 
 /* --------------------------------------------------------------------------- */
 /* getUsageNotes:                                                              */
 /* --------------------------------------------------------------------------- */
 private String getUsageNotes()
 {
  String s = "\nMongoLogAnalyzer parses and analyses the log files produced by mongod.";
  return s;
 }
}