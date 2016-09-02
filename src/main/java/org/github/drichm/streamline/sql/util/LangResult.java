package org.github.drichm.streamline.sql.util;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ResultSet readers that check for null and return java.lang Objects for base types 
 */
public class LangResult
{
  //===========================================================================

  /** Return Short instance if value is present, null if not present */
  static public Short asShort( ResultSet rs, int columnIndex ) throws SQLException
  {
    short s = rs.getShort( columnIndex );
    return rs.wasNull() ? null : new Short(s);
  }

  /** Return Integer instance if value is present, null if not present */
  static public Integer asInteger( ResultSet rs, int columnIndex ) throws SQLException
  {
    int i = rs.getInt( columnIndex );
    return rs.wasNull() ? null : new Integer(i);
  }

  /** Return Long instance if value is present, null if not present */
  static public Long asLong( ResultSet rs, int columnIndex ) throws SQLException
  {
    long l = rs.getLong( columnIndex );
    return rs.wasNull() ? null : new Long(l);
  }

  /** Return Float instance if value is present, null if not present */
  static public Float asFloat( ResultSet rs, int columnIndex ) throws SQLException
  {
    float f = rs.getFloat( columnIndex );
    return rs.wasNull() ? null : new Float(f);
  }

  /** Return Double instance if value is present, null if not present */
  static public Double asDouble( ResultSet rs, int columnIndex ) throws SQLException
  {
    double d = rs.getDouble( columnIndex );
    return rs.wasNull() ? null : new Double(d);
  }

} // end of class LangResult
