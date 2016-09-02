package org.github.drichm.streamline.sql.util;

import java.util.Calendar;
import java.util.Date;

/**
 * 
 */
public class Helpers
{
  //===========================================================================

  /** Is 'a' null or empty */
  static public boolean isEmpty( Object[] a )
  {
    return (a == null) || (a.length == 0);
  }

  
  //===========================================================================
  
  /** Fields to set for Midnight, when these fields are zero then JDBC stores a DATE not a TIMESTAMP */
  static private final int[] MIDNIGHT_FIELDS =
  {
    Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND
  };

  /** Does given date have a time */
  static public boolean hasTime( Date date )
  {
    if ( date == null )
      return false;

    Calendar c = Calendar.getInstance();
    c.setTime( date );

    for ( int fld: MIDNIGHT_FIELDS )
      if ( c.get( fld ) != 0 )
        return true;

    return false;

  } // end of hasTime

} // end of class Helpers
