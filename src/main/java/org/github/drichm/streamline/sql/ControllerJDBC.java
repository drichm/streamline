package org.github.drichm.streamline.sql;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.github.drichm.streamline.sql.util.Helpers;

/**
 * How to set parameters into a statement, format values, log status, track timings.
 *<p>
 * Override this class to add your own controlling behaviour
 */
public class ControllerJDBC
{
  //===========================================================================
  // Setting parameters into a PreparedStatement
  
  /**
   * Set parameter value on prepared statement
   * 
   * @param parameterIndex 1-based index
   * @param value Parameter value to set
   */
  public void set( PreparedStatement ps, int parameterIndex, Object value )
  {
    try
    {
      if ( value == null )
        ps.setNull( parameterIndex, java.sql.Types.NULL );
      else
      if ( value instanceof Date )
      { // SQL Dates/Timestamps can be a real-pain - set them explicitly

        if ( value instanceof java.sql.Timestamp )
          ps.setTimestamp( parameterIndex, (java.sql.Timestamp) value );
        else
        if ( value instanceof java.sql.Time )
          ps.setTime     ( parameterIndex, (java.sql.Time) value );
        else
        if ( value instanceof java.sql.Date )
          ps.setDate     ( parameterIndex, (java.sql.Date) value );
        else
        {
          Date date = (Date) value;
  
          if ( Helpers.hasTime( date ) )
            ps.setTimestamp( parameterIndex, new java.sql.Timestamp( date.getTime() ) );
          else
            ps.setDate     ( parameterIndex, new java.sql.Date     ( date.getTime() ) );
        }
      }
      else
      if ( value instanceof Blob )
        ps.setBlob  ( parameterIndex, (Blob) value );
      else
      if ( value instanceof Clob )
        ps.setClob  ( parameterIndex, (Clob) value );
      else
        // catch all that includes java.lang types
        ps.setObject( parameterIndex, value );
    }
    catch (SQLException e)
    {
      throw new CheckedException( "Bad value: index="+parameterIndex+", value="+value, e );
    }

  } // end of set
  
  
  /** Set parameter values on prepared statement */
  public void set( PreparedStatement ps, int indexFirst, Object... parameters )
  {
    if ( parameters != null )
      for ( int i=0 ; i < parameters.length ; i++ )
        set( ps, i+indexFirst, parameters[i] );
  }


  //===========================================================================
  // Formatting values for logging/debug
  
  static protected final DateFormatSymbols 
    dateSymbols   = new DateFormatSymbols( Locale.US );

  static protected SimpleDateFormat formatDate     ()  { return new SimpleDateFormat( "'yyyy-MM-dd'"         , dateSymbols ); }
  static protected SimpleDateFormat formatTimestamp()  { return new SimpleDateFormat( "'yyyy-MM-dd hh:mm:ss'", dateSymbols ); }

  /** Format value as an equivalent SQL string, returns null if not possible */
  public String format( Object value )
  {
    if ( value == null )
      return "NULL";
    else
    if ( value instanceof Date )
    {
      if ( Helpers.hasTime( (Date) value ) )
        return formatTimestamp().format( (Date) value );
      else
        return formatDate     ().format( (Date) value );
    }
    else
    if ( value instanceof String )
      return "'" + ((String) value).replace( "'", "''" ) + "'";
    else
    if ( value instanceof Number )
      return ( (Number) value ).toString();
    else
      // unable to format
      return null;

  } // end of format


  //===========================================================================
  // Events for Timing / Logging
  
  /** New DataSource Connection created */ 
  public void notifyConnect( java.sql.Connection conn )
  {
  }
  
  /** DataSource Connection closed */ 
  public void notifyDisconnect( java.sql.Connection conn )
  {
  }

  /** About to execute SQL */
  public void notifyStart( SqlStatement sql )
  {
  }

  /** SQL execution failed, will throw a {@link CheckedException} after this call */
  public void notifyFail( SqlStatement sql, SQLException e )
  {
  }

  /** SQL execution completed, JDBC objects closed */
  public void notifyComplete( SqlStatement sql )
  {
  }

} // end of class Parameters
