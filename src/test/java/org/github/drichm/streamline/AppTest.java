package org.github.drichm.streamline;

import org.github.drichm.streamline.sql.StreamlineJDBC;
import org.github.drichm.streamline.sql.StreamlineJDBC.Connection;
import org.h2.jdbcx.JdbcConnectionPool;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
  // ===========================================================================
  
  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public AppTest(String testName)
  {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  static public Test suite()
  {
    return new TestSuite(AppTest.class);
  }

  // ===========================================================================
  
  public TestResult eclipseTest()  { return super.run(); }

  @Override protected void runTest()
  {
    try ( StreamlineJDBC sl = new StreamlineJDBC( JdbcConnectionPool.create("jdbc:h2:mem:test", "test", "test") ) )
    {
      run1(sl);
      run2(sl);
    }
  }

  
  // ===========================================================================

  static private final String SQL = "select table_catalog, table_schema, table_name, table_type from INFORMATION_SCHEMA.TABLES";

  protected void run1( StreamlineJDBC sl )
  {
    sl.stream( rs -> rs.getString(2) + "." + rs.getString(3), SQL ).forEach( this::log );

    sl.stream( rs -> rs.getString(2) + "." + rs.getString(3), SQL ).findFirst().ifPresent( this::log );

    // sl.storedProcedure( "sp_who2" );
  }

  static private final String CREATE = "create table test ( x int, s varchar(50) );",
      INSERT = "insert into test(x,s) values (?,?)",
      COUNT = "select sum(x) from test where s=?",
      SELECT = "select x from test where s=?", DROP = "drop table test;";

  protected void run2( StreamlineJDBC sl )
  {
    try (Connection conn = sl.connection())
    {
      // Connection conn = sl.connection();

      conn.execute(CREATE);
      conn.execute(INSERT, 100, "a");
      conn.execute(INSERT, 200, "b");
      conn.execute(INSERT, 300, "a");

      assertEquals( "Bad sum on 'a'", conn.stream(rs -> rs.getInt(1), COUNT, "a").findFirst().get(), new Integer(400) );
      assertEquals( "Bad sum on 'b'", conn.stream(rs -> rs.getInt(1), COUNT, "b").findFirst().get(), new Integer(200) );

      assertEquals( "Bad row counton 'a'",  conn.list(rs -> rs.getInt(1), SELECT, "a").size(), 2 );
      assertEquals( "Bad row count on 'b'", conn.list(rs -> rs.getInt(1), SELECT, "b").size(), 1 );

      conn.execute(DROP);
    }
  }

  
  protected void log( String message )
  {
    System.out.println( message );
  }

}
