package org.github.drichm.streamline.sql;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

/**
 * A streamline approach to JDBC
 *<p>
 * <strong>IMPORTANT</strong>
 * In Java 8 {@link Stream} short-circuiting operations 
 * [ 
 *   {@link Stream#limit(long)}
 * , {@link Stream#anyMatch(java.util.function.Predicate)}
 * , {@link Stream#allMatch(java.util.function.Predicate)}
 * , {@link Stream#noneMatch(java.util.function.Predicate)}
 * , {@link Stream#findFirst()}
 * , {@link Stream#findAny()}
 * ]
 * do not close the stream. This means open {@link Statement}s and {@link ResultSet}s
 * will not be closed when those operations are used.
 * <p>
 * We try to mitigate this by tracking connections and open streams and ensure that
 * they are closed, this is done via {@link AutoCloseable} resource management
 * on an instance of this class.
 * 
 * <pre>
 * try( StreamlineJDBC sl = new StreamlineJDBC(...) )
 * {
 *   return sl.stream( parser, "select x from y" ).findFirst();
 * }
 * </pre>
 */
public class StreamlineJDBC implements AutoCloseable
{
  //===========================================================================

  /** A parser that converts the current ResultSet row into an instance of T */
  @FunctionalInterface
  static public interface RowParser<T>
  {
    /**
     * Parse current row in rs - DO NOT CALL rs.next()...
     *
     * @return Parsed instance - null to ignore the row
     */
    public T parse( ResultSet rs ) throws SQLException;

  } // end of interface RowParser
  

  //===========================================================================
  
  private final DataSource     ds;
  private final Connection     global;
  private final ControllerJDBC controller;
  
  /** Construct on a (preferably pooling) DataSource, new connections will be created on demand */
  public  StreamlineJDBC( DataSource ds )                                        { this( ds, null, new ControllerJDBC() ); }

  /** Construct on a (preferably pooling) DataSource, new connections will be created on demand */
  public  StreamlineJDBC( DataSource ds, ControllerJDBC controller )             { this( ds, null, controller ); }
  
  /** Construct on a fixed connection, connection will NEVER be closed by this class */
  public  StreamlineJDBC( java.sql.Connection jdbc )                             { this( null, jdbc, new ControllerJDBC() ); }

  /** Construct on a fixed connection, connection will NEVER be closed by this class */
  public  StreamlineJDBC( java.sql.Connection jdbc, ControllerJDBC controller )  { this( null, jdbc, controller ); }

  /** Main constructor */
  private StreamlineJDBC( DataSource ds, java.sql.Connection jdbc, ControllerJDBC controller )
  {
    this.ds         = ds;
    this.global     = jdbc == null ? null : new Connection( jdbc, false );
    this.controller = controller != null ? controller : new ControllerJDBC();
  }

  
  //===========================================================================
  // Convenience functions to avoid explicit connection access

  /** Execute SELECT to end, collect all non-null values and return in a possibly empty list of T */
  public <T> List<T> list( RowParser<T> parser, CharSequence sql, Object... parameters )
  {
    try ( Connection conn = connection() )
    {
      return conn.list( parser, new SqlStatement( sql, parameters ) );
    }
  }
  
  /** Execute SELECT to end, collect all non-null values and return in a possibly empty list of T */
  public <T> List<T> list( RowParser<T> parser, SqlStatement sql )
  {
    try ( Connection conn = connection() )
    {
      return conn.list( parser, sql );
    }
  }

  
  /** Stream ResultSet, ResultSet is closed when last row is returned */
  public <T> Stream<T> stream( RowParser<T> parser, CharSequence sql, Object... parameters )
  {
    // returned stream will close connection when stream is has a terminal operation
    return connection().stream( parser, sql, parameters );
  }

  /** Stream ResultSet, ResultSet is closed when last row is returned */
  public <T> Stream<T> stream( RowParser<T> parser, SqlStatement sql )
  {
    // returned stream will close connection when stream is has a terminal operation
    return connection().stream( parser, sql );
  }

  /** Execute a statement, no return values expected */
  public void execute( CharSequence sql, Object... parameters )
  {
    try ( Connection conn = connection() )
    {
      conn.execute( sql, parameters );
    }
  }

  /** Execute a statement, no return values expected */
  public void execute( SqlStatement sql )
  {
    try ( Connection conn = connection() )
    {
      conn.execute( sql );
    }
  }

  /** Execute a stored procedure by name with arguments, return its integer return code */
  public Integer storedProcedure( String name, Object... args )
  {
    try ( Connection conn = connection() )
    {
      return conn.storedProcedure( name, args );
    }
  }

  
  //===========================================================================
  // Connection management
  
  private final Map<Connection,Connection>                     openConnection = new ConcurrentHashMap<>();
  private final Map<ResultSpliterator<?>,ResultSpliterator<?>> openStream     = new ConcurrentHashMap<>();
  
  public int countOpenConnections()  { return openConnection.size(); }
  public int countOpenStreams    ()  { return openStream    .size(); }

  /** Return a connection to execute upon */
  public Connection connection()
  {
    try
    {
      if ( global != null )
        return global;
      else
      {
        Connection conn = new Connection( ds.getConnection(), true );
        openConnection.put( conn, conn );
        return conn;
      }
    }
    catch ( SQLException e )
    {
      throw new CheckedException( e );
    }
  }
  
  protected void closed( Connection conn )
  {
    controller    .notifyDisconnect( conn.jdbc );
    openConnection.remove( conn );
  }
  
  @Override public void close()
  {
    openStream    .keySet().forEach( ResultSpliterator::close );
    openConnection.keySet().forEach( Connection::close );
  }
  

  //===========================================================================

  /** Connection handling */
  public class Connection implements AutoCloseable
  {
    public final java.sql.Connection jdbc;
    public final boolean             autoclose;
    
    protected Connection( java.sql.Connection jdbc, boolean autoclose )
    { 
      this.jdbc      = jdbc;
      this.autoclose = autoclose;
      
      if ( autoclose )
        controller.notifyConnect( jdbc );
    }
    
    // AutoCloseable
    @Override public void close()
    { 
      if ( autoclose )
        try
        {
          if ( !jdbc.isClosed() )
          {
            closed( this ); 
            jdbc.close();
          } 
        }
        catch (SQLException e)  { throw new CheckedException(e); }
    }


    /** Is auto-commit on or off */
    public boolean isAutoCommit()                      { try { return jdbc.getAutoCommit();           } catch (SQLException e) { throw new CheckedException(e); } }

    /** Turn connection auto-commit on or off */
    public void    autoCommit  ( boolean autoCommit )  { try { jdbc.setAutoCommit( autoCommit );      } catch (SQLException e) { throw new CheckedException(e); } }

    /** Is connection readonly */
    public boolean isReadonly  ()                      { try { return jdbc.isReadOnly();              } catch (SQLException e) { throw new CheckedException(e); } }

    /** Make connection readonly, or not */
    public void    readonly    ( boolean readOnly )    { try { jdbc.setReadOnly( readOnly );          } catch (SQLException e) { throw new CheckedException(e); } }

    /** Commit any open transaction */
    public void    commit      ()                      { try { if ( isCommitable() ) jdbc.commit();   } catch (SQLException e) { throw new CheckedException(e); } }

    /** Rollback any open transaction */
    public void    rollback    ()                      { try { if ( isCommitable() ) jdbc.rollback(); } catch (SQLException e) { throw new CheckedException(e); } }

    /** Rollback any open transaction */
    public void    rollback( Savepoint savepoint )     { if ( savepoint != null )  rollback( savepoint );  else  rollback(); }

    /** Mark a savepoint - this will also turn off auto-commit */ 
    public Savepoint setSavepoint( String name )
    {
      if ( isAutoCommit() )
        autoCommit( false );

      try { return jdbc.setSavepoint( name ); } catch (SQLException e) { throw new CheckedException(e); }
    }
    
    /** Can commits or rollbacks be executed on this connection */
    protected boolean isCommitable() throws SQLException
    {
      return !jdbc.isClosed() && !jdbc.isReadOnly() && !jdbc.getAutoCommit();
    }

    
    /** Execute SELECT to end, collect all non-null values and return in a possibly empty list of T */
    public <T> List<T> list( RowParser<T> parser, CharSequence sql, Object... parameters )
    {
      return statementList( this, parser, new SqlStatement( sql, parameters ) );
    }
    
    /** Execute SELECT to end, collect all non-null values and return in a possibly empty list of T */
    public <T> List<T> list( RowParser<T> parser, SqlStatement sql )
    {
      return statementList( this, parser, sql );
    }

    /** Stream ResultSet, ResultSet is closed when last row is returned or stream is closed after a terminal operation */
    public <T> Stream<T> stream( RowParser<T> parser, CharSequence sql, Object... parameters )
    {
      return stream( parser, new SqlStatement( sql, parameters ) );
    }

    /** Stream ResultSet, ResultSet is closed when last row is returned or stream is closed after a terminal operation */
    public <T> Stream<T> stream( RowParser<T> parser, SqlStatement sql )
    {
      ResultSpliterator<T> rs = new ResultSpliterator<T>( this, parser, sql );

      return StreamSupport.stream( rs, false ).onClose( rs::close );
    }

    /** Execute a statement, no return values expected */
    public void execute( CharSequence sql, Object... parameters )
    {
      statementExecute( this, new SqlStatement( sql, parameters ) );
    }

    /** Execute a statement, no return values expected */
    public void execute( SqlStatement sql )
    {
      statementExecute( this, sql );
    }

    /** Execute a stored procedure by name with arguments, return its integer return code */
    public Integer storedProcedure( String name, Object... args )
    {
      return executeStoredProcedure( this, name, args );
    }
    

  } // end of inner-class Connection
  
  
  
  //===========================================================================
  // Statement
  

  /**
   * Compile and execute an SQL statement
   *<p>
   * If parameters are not defined then a normal statement is executed,
   * otherwise a PreparedStatement is created, filled in with parameters and then
   * executed.
   *
   * @return Statement instance, which must be closed by caller
   */
  protected Statement statement( Connection conn, SqlStatement sql )
  {
    try
    {
      if ( sql.simple )
      {
        Statement stmt = conn.jdbc.createStatement();
        stmt.execute( sql.sql.toString() );
        return stmt;
      }
      else
      {
        PreparedStatement stmt = conn.jdbc.prepareStatement( sql.sql.toString() );
        controller.set( stmt, 1, sql.parameters );
        stmt.execute();
        return stmt;
      }
    }
    catch ( SQLException e )
    {
      throw new CheckedException( sql, e );
    }

  } // end of statement

  
  /** Execute SELECT to end, collect all non-null values and return in a possibly empty list of T */
  protected <T> List<T> statementList( Connection conn, RowParser<T> parser, SqlStatement sql )
  {
    ArrayList<T> list = new ArrayList<T>();
    
    controller.notifyStart( sql );

    try (
          Statement stmt = statement( conn, sql ); 
          ResultSet rs   = stmt.getResultSet();
        )
    {
      if ( rs != null )
      {
        while ( rs.next() )
        {
          T t = parser.parse( rs );
  
          if ( t != null )
            list.add( t );
        }
      }
    }
    catch ( SQLException e )
    {
      controller.notifyFail( sql, e );
      throw new CheckedException( sql, e );
    }

    controller.notifyComplete( sql );

    return list;

  } // end of statementList


  /** Execute a statement, no return values expected */
  protected void statementExecute( Connection conn, SqlStatement sql )
  {
    controller.notifyStart( sql );

    try ( Statement stmt = statement( conn, sql ) )
    {
    }
    catch ( SQLException e )
    {
      controller.notifyFail( sql, e );
      throw new CheckedException( sql, e );
    }

    controller.notifyComplete( sql );

  } // end of statementExecute


  /** Stream support */
  protected class ResultSpliterator<T> implements Spliterator<T>
  {
    private final Connection   conn;
    private final RowParser<T> parser;
    private final SqlStatement sql;
    private final Statement    stmt; 
    private final ResultSet    rs;
    
    private boolean closed = false;

    protected ResultSpliterator( Connection conn, RowParser<T> parser, SqlStatement sql )
    {
      openStream.put( this,  this );

      controller.notifyStart( sql );

      try
      {
        this.conn   = conn;
        this.parser = parser;
        this.sql    = sql;
        this.stmt   = statement( conn, sql ); 
        this.rs     = stmt.getResultSet();
      }
      catch ( SQLException e )
      {
        throw new CheckedException( sql, e );
      }
    }

    @Override public int            characteristics()  { return IMMUTABLE | DISTINCT; }
    @Override public long           estimateSize   ()  { return Long.MAX_VALUE;  }
    @Override public Spliterator<T> trySplit       ()  { return null; }   // cannot be split


    @Override public boolean tryAdvance( Consumer<? super T> action )
    {
      try
      {
        if ( closed )
          return false;
        else
        if ( !rs.next() )
        { // no more rows, close all JDBC stuff we can
          close();
          return false;
        }
        else
        {
          action.accept( parser.parse( rs ) );
          return true;
        }
      }
      catch ( SQLException e )
      {
        controller.notifyFail( sql, e );
        throw new CheckedException( sql, e );
      }
    }
    
    /** Close/cleanup all JDBC objects */
    public void close()
    {
      try
      {
        if ( !closed )
        {
          closed = true;

          openStream.remove( this );

          rs  .close();
          stmt.close();
          conn.close();
          
          controller.notifyComplete( sql );
        }
      }
      catch ( SQLException e )
      {
        throw new CheckedException( sql, e );
      }
    }
    
  } // end of inner-class ResultSpliterator<T>

  
  //===========================================================================
  // Callable Statement

  /** 
   * Execute a stored procedure by name with arguments, return its integer return code
   *<p>
   * Sorry, INOUT and OUT parameters are not currently supported
   */
  protected int executeStoredProcedure( Connection conn, String name, Object... args )
  {
    SqlStatement sql = new SqlStatement(
         "{? = call " + name + "(" + Arrays.stream( args ).map( x -> "?" ).collect( Collectors.joining( "," ) ) + ")}"
         , args
         );
    
    controller.notifyStart( sql );
    
    try ( CallableStatement cstmt = conn.jdbc.prepareCall( sql.sql.toString() ) ) 
    {
       cstmt.registerOutParameter( 1, java.sql.Types.INTEGER );

       controller.set( cstmt, 2, args );

       cstmt.execute();

       controller.notifyComplete( sql );

       return cstmt.getInt( 1 );
    }
    catch ( SQLException e )
    {
      controller.notifyFail( sql, e );

      throw new CheckedException( sql, e );
    }
  }

} // end of class StreamlineJDBC
