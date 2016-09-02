package org.github.drichm.streamline.sql;

/**
 * A thrown checked exception, wrapped in a RuntimeException
 */
public class CheckedException extends RuntimeException
{
  private static final long serialVersionUID = 5999579559664138204L;

  //===========================================================================
  
  public CheckedException( String message, Throwable t )
  {
    super( message, t );
  }

  public CheckedException( SqlStatement sql, Throwable t )
  {
    super( sql.sql.toString(), t );
  }

  public CheckedException( Throwable t )
  {
    super( t );
  }
  
} // end of class CheckedException
