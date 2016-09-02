package org.github.drichm.streamline.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.github.drichm.streamline.sql.util.Helpers;

/**
 * An SQL statement and its parameters 
 */
public class SqlStatement
{
  //===========================================================================
  // Configuration

  public final CharSequence sql;
  public final Object[]     parameters;
  public final boolean      simple;

  public SqlStatement( CharSequence sql, List<Object> parameters )
  {
    this( sql, (parameters == null) ? null : parameters.toArray() );
  }

  public SqlStatement( CharSequence sql, Object... parameters )
  {
    this.sql        = sql;
    this.parameters = parameters;
    this.simple     = Helpers.isEmpty( parameters );
  }
  
  
  //===========================================================================

  /**
   * Format SQL string "?" placeholders with formatted parameters
   *<p>
   * IMPORTANT:
   * This is for logging and debug purposes only, do not execute the returned statement
   * if you wish to avoid SQL Injection attacks
   * 
   * @see https://www.owasp.org/index.php/SQL_Injection
   */
  public SqlStatement format( ControllerJDBC param )
  {
    return format( param, "?" );
  }

  /**
   * Format SQL string 'placeholder' placeholders with formatted parameters
   *<p>
   * IMPORTANT:
   * This is for logging and debug purposes only, do not execute the returned statement
   * if you wish to avoid SQL Injection attacks
   * 
   * @see https://www.owasp.org/index.php/SQL_Injection
   */
  public SqlStatement format( ControllerJDBC param, String placeholder )
  {
    List<Object> args  = new ArrayList<Object>();
    String       query = sql.toString();

    if ( query.indexOf( placeholder ) < 0 )
      return new SqlStatement( query, args );

    StringBuilder b     = new StringBuilder();
    String[]      split = query.split( Pattern.quote( placeholder ) );

    for ( int i=0 ; i < split.length ; i++ )
    {
      b.append( split[i] );

      if ( i < parameters.length )
      {
        Object value = parameters[i];
        String as    = param.format( value );

        if ( as != null )
          b.append( as );
        else
        {
          b.append( placeholder );
          args.add( value );
        }
      }
    }

    return new SqlStatement( b, args );

  } // end of format

} // end of class Statement
