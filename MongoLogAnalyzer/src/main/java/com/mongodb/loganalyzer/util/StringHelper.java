package com.mongodb.loganalyzer.util;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringHelper 
{
 // Allow logging.
 private static final Logger LOG = LoggerFactory.getLogger(StringHelper.class);
 
 // Default string for dumping null values.
 public static final String NULL_STRING = "<null>";

 /**
  * Dump the contents of the parameter to a string using reflection. The content
  * is represented as a list of field/value pairs. See Apache documentation for
  * details.
  * <p>
  * BE CAREFUL NOT TO CALL THIS METHOD ON OBJECTS THAT CONTAIN PASSWORDS.
  * 
  * @param obj the object whose contents should be written to a strin. 
  *        Null values are allowed.
  * @return a string that represent the input object's fields and values 
  *         written on multiple lines, one for each field.  This output is
  *         appropriate for logging, debugging, or anyplace where humans will
  *         read it.  The output includes the virtual address of the object,
  *         so different objects with the exact same content can be distinguished.
  * 
  * Should use Apache commons bean-utils or call lang directly
  */
 public static String toString(Object obj) 
 {
  if (obj == null) return NULL_STRING;
  return ReflectionToStringBuilder.toString(obj, ToStringStyle.MULTI_LINE_STYLE);
 }

 /** Dump the contents of the parameter to a string using reflection. The content
  * is represented as a list of field/value pairs. See Apache documentation for
  * details.
  * <p>
  * BE CAREFUL NOT TO CALL THIS METHOD ON OBJECTS THAT CONTAIN PASSWORDS.
  * 
  * @param obj the object whose contents should be written to a strin. 
  *        Null values are allowed.
  * @return a string that represent the input object's fields and values 
  *         written on a single line and without any address information to
  *         distinguish two objects of the same type with the same content.
  *         This output is appropriate for programmatic comparison of two
  *         objects of the same type.
  * 
  * Should use Apache commons bean-utils or call lang directly
  */
 public static String toComparableString(Object obj) 
 {
  if (obj == null) return NULL_STRING;
  return ReflectionToStringBuilder.toString(obj, ToStringStyle.SHORT_PREFIX_STYLE);
 }
}