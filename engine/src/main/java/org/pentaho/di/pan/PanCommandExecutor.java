/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.pan;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.pentaho.di.base.AbstractBaseCommandExecutor;
import org.pentaho.di.base.CommandExecutorCodes;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.FileUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.engine.api.model.Transformation;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.RepositoryOperation;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PanCommandExecutor extends AbstractBaseCommandExecutor {

  public PanCommandExecutor( Class<?> pkgClazz ) {
    this( pkgClazz, new LogChannel( Pan.STRING_PAN ) );
  }

  public PanCommandExecutor( Class<?> pkgClazz, LogChannelInterface log ) {
    setPkgClazz( pkgClazz );
    setLog( log );
  }

  final String TMP_BASE_DIR = ".tmp";

  public Result execute( String repoName, String noRepo, String username, String trustUser, String password, String dirName,
                         String filename, String jarFile, String transName, String xml, String listTrans, String listDirs, String exportRepo,
                         String initialDir, String listRepos, String safemode, String metrics, String listParams, String resultSetStepName,
                         String resultSetCopyNumber, NamedParams params, String[] arguments ) throws Throwable {

    getLog().logMinimal( BaseMessages.getString( getPkgClazz(), "Pan.Log.StartingToRun" ) );

    Date start = Calendar.getInstance().getTime(); // capture execution start time

    logDebug( "Pan.Log.AllocatteNewTrans" );

    Trans trans = null;

    // In case we use a repository...
    Repository repository = null;

    try {

      if ( getMetaStore() == null ) {
        setMetaStore( createDefaultMetastore() );
      }

      logDebug( "Pan.Log.StartingToLookOptions" );

      // Read kettle transformation specified
      if ( !Utils.isEmpty( repoName ) || !Utils.isEmpty( filename ) || !Utils.isEmpty( jarFile ) || !Utils.isEmpty( xml ) ) {

        logDebug( "Pan.Log.ParsingCommandline" );

        if ( !Utils.isEmpty( repoName ) && !isEnabled( noRepo ) ) {

          /**
           * if set, _trust_user_ needs to be considered. See pur-plugin's:
           *
           * @link https://github.com/pentaho/pentaho-kettle/blob/8.0.0.0-R/plugins/pur/core/src/main/java/org/pentaho/di/repository/pur/PurRepositoryConnector.java#L97-L101
           * @link https://github.com/pentaho/pentaho-kettle/blob/8.0.0.0-R/plugins/pur/core/src/main/java/org/pentaho/di/repository/pur/WebServiceManager.java#L130-L133
           */
          if ( isEnabled( trustUser ) ) {
            System.setProperty( "pentaho.repository.client.attemptTrust", YES );
          }

          // In case we use a repository...
          // some commands are to load a Trans from the repo; others are merely to print some repo-related information
          RepositoryMeta repositoryMeta = loadRepositoryConnection( repoName, "Pan.Log.LoadingAvailableRep", "Pan.Error.NoRepsDefined", "Pan.Log.FindingRep" );

          repository = establishRepositoryConnection( repositoryMeta, username, password, RepositoryOperation.EXECUTE_TRANSFORMATION );

          trans = executeRepositoryBasedCommand( repository, repositoryMeta, dirName, transName, xml, listTrans, listDirs, exportRepo );
        }

        // Try to load the transformation from file, even if it failed to load from the repository
        // You could implement some fail-over mechanism this way.
        if ( trans == null ) {
          trans = executeFilesystemBasedCommand( initialDir, filename, jarFile, xml );
        }

      }

      if ( isEnabled( listRepos ) ) {
        printRepositories( loadRepositoryInfo( "Pan.Log.LoadingAvailableRep", "Pan.Error.NoRepsDefined" ) ); // list the repositories placed at repositories.xml
      }

    } catch ( Exception e ) {

      trans = null;

      System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.ProcessStopError", e.getMessage() ) );
      e.printStackTrace();
      if ( repository != null ) {
        repository.disconnect();
      }
      return exitWithStatus( CommandExecutorCodes.Pan.ERRORS_DURING_PROCESSING.getCode() );
    }

    if ( trans == null ) {

      if ( !isEnabled( listTrans ) && !isEnabled( listDirs ) && !isEnabled( listRepos ) && Utils.isEmpty( exportRepo ) ) {

        System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.CanNotLoadTrans" ) );
        return exitWithStatus( CommandExecutorCodes.Pan.COULD_NOT_LOAD_TRANS.getCode() );
      } else {
        return exitWithStatus( CommandExecutorCodes.Pan.SUCCESS.getCode() );
      }
    }

    try {

      trans.setLogLevel( getLog().getLogLevel() );
      configureParameters( trans, params, trans.getTransMeta() );

      trans.setSafeModeEnabled( isEnabled( safemode ) ); // run in safe mode if requested
      trans.setGatheringMetrics( isEnabled( metrics ) ); // enable kettle metric gathering if requested

      // List the parameters defined in this transformation, and then simply exit
      if ( isEnabled( listParams ) ) {

        printTransformationParameters( trans );

        // stop right here...
        return exitWithStatus( CommandExecutorCodes.Pan.COULD_NOT_LOAD_TRANS.getCode() ); // same as the other list options
      }

      final List<RowMetaAndData> rows = new ArrayList<RowMetaAndData>(  );

      // allocate & run the required sub-threads
      try {
        trans.prepareExecution( arguments );

        if ( !StringUtils.isEmpty( resultSetStepName ) ) {

          int copyNr = NumberUtils.isNumber( resultSetCopyNumber ) ? Integer.parseInt( resultSetCopyNumber ) : 0 /* default */;

          logDebug( "Collecting result-set for step '" +  resultSetStepName + "' and copy number " + copyNr );

          StepInterface step = null;
          if ( ( step = trans.findRunThread( resultSetStepName ) ) != null && step.getCopy() == copyNr ) {
            step.addRowListener( new RowAdapter() {
              @Override
              public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] data ) throws KettleStepException {
                rows.add( new RowMetaAndData( rowMeta, data ) );
              }
            } );
          }
        }

        trans.startThreads();

      } catch ( KettleException ke ) {
        logDebug( ke.getLocalizedMessage() );
        System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.UnablePrepareInitTrans" ) );
        return exitWithStatus( CommandExecutorCodes.Pan.UNABLE_TO_PREP_INIT_TRANS.getCode() );
      }

      waitUntilFinished( trans, 100 ); // Give the transformation up to 10 seconds to finish execution

      if ( trans.isRunning() ) {
        getLog().logError( BaseMessages.getString( getPkgClazz(), "Pan.Log.NotStopping" ) );
      }

      getLog().logMinimal( BaseMessages.getString( getPkgClazz(), "Pan.Log.Finished" ) );
      Date stop = Calendar.getInstance().getTime(); // capture execution stop time

      trans.setResultRows( rows );
      setResult( trans.getResult() ); // get the execution result

      int completionTimeSeconds = calculateAndPrintElapsedTime( start, stop, "Pan.Log.StartStop", "Pan.Log.ProcessingEndAfter",
              "Pan.Log.ProcessingEndAfterLong", "Pan.Log.ProcessingEndAfterLonger", "Pan.Log.ProcessingEndAfterLongest" );
      getResult().setElapsedTimeMillis( stop.getTime() - start.getTime() );

      if ( getResult().getNrErrors() == 0 ) {

        trans.printStats( completionTimeSeconds );
        return exitWithStatus( CommandExecutorCodes.Pan.SUCCESS.getCode() );

      } else {

        String transJVMExitCode = trans.getVariable( Const.KETTLE_TRANS_PAN_JVM_EXIT_CODE );

        // If the trans has a return code to return to the OS, then we exit with that
        if ( !Utils.isEmpty( transJVMExitCode ) ) {

          try {
            return exitWithStatus( Integer.parseInt( transJVMExitCode ) );

          } catch ( NumberFormatException nfe ) {
            getLog().logError( BaseMessages.getString( getPkgClazz(), "Pan.Error.TransJVMExitCodeInvalid",
                    Const.KETTLE_TRANS_PAN_JVM_EXIT_CODE, transJVMExitCode ) );
            getLog().logError( BaseMessages.getString( getPkgClazz(), "Pan.Log.JVMExitCode", "1" ) );
            return exitWithStatus( CommandExecutorCodes.Pan.ERRORS_DURING_PROCESSING.getCode() );
          }

        } else {
          // the trans does not have a return code.
          return exitWithStatus( CommandExecutorCodes.Pan.ERRORS_DURING_PROCESSING.getCode() );
        }
      }

    } catch ( KettleException ke ) {

      System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Log.ErrorOccurred", "" + ke.getMessage() ) );
      getLog().logError( BaseMessages.getString( getPkgClazz(), "Pan.Log.UnexpectedErrorOccurred", "" + ke.getMessage() ) );

      return exitWithStatus( CommandExecutorCodes.Pan.UNEXPECTED_ERROR.getCode() );

    } finally {
      if ( repository != null ) {
        repository.disconnect();
      }
      if ( isEnabled( trustUser ) ) {
        System.clearProperty( "pentaho.repository.client.attemptTrust" ); // we set it, now we sanitize it
      }
    }
  }

  public int printVersion() {
    printVersion( "Pan.Log.KettleVersion" );
    return CommandExecutorCodes.Pan.KETTLE_VERSION_PRINT.getCode();
  }

  public Trans executeRepositoryBasedCommand( Repository repository, RepositoryMeta repositoryMeta, String dirName,
                                              String transName, String xml, String listTrans, String listDirs, String exportRepo ) throws Exception {

    try {

      if ( repository != null && repositoryMeta != null ) {
        // Define and connect to the repository...
        logDebug( "Pan.Log.Allocate&ConnectRep" );

        // Default is the root directory
        RepositoryDirectoryInterface directory = repository.loadRepositoryDirectoryTree();

        // Add the IMetaStore of the repository to our delegation
        if ( repository.getMetaStore() != null && getMetaStore() != null ) {
          getMetaStore().addMetaStore( repository.getMetaStore() );
        }

        // Find the directory name if one is specified...
        if ( !Utils.isEmpty( dirName ) ) {
          directory = directory.findDirectory( dirName );
        }

        if ( directory != null ) {
          // Check username, password
          logDebug( "Pan.Log.CheckSuppliedUserPass" );

          // transname is not empty ? then command it to load a transformation
          if ( !Utils.isEmpty( transName ) ) {

            logDebug( "Pan.Log.LoadTransInfo" );
            TransMeta transMeta = repository.loadTransformation( transName, directory, null, true, null );

            logDebug( "Pan.Log.AllocateTrans" );
            Trans trans = new Trans( transMeta );
            trans.setRepository( repository );
            trans.setMetaStore( getMetaStore() );

            return trans; // return transformation loaded from the repo

          // xml input is not empty ? then command it to load a transformation
          } else if ( !Utils.isEmpty( xml )  ) {

            logDebug( "Pan.Log.LoadTransInfo" );
            TransMeta transMeta = new TransMeta( new ByteArrayInputStream( xml.getBytes() ), repository, true, null, null );

            logDebug( "Pan.Log.AllocateTrans" );
            Trans trans = new Trans( transMeta );
            trans.setRepository( repository );
            trans.setMetaStore( getMetaStore() );

            return trans; // return transformation loaded from the repo

          } else if ( isEnabled( listTrans ) ) {

            printRepositoryStoredTransformations( repository, directory ); // List the transformations in the repository

          } else if ( isEnabled( listDirs ) ) {

            printRepositoryDirectories( repository, directory ); // List the directories in the repository

          } else if ( !Utils.isEmpty( exportRepo ) ) {

            // Export the repository
            System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Log.ExportingObjectsRepToFile", "" + exportRepo ) );
            repository.getExporter().exportAllObjects( null, exportRepo, directory, "all" );
            System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Log.FinishedExportObjectsRepToFile", "" + exportRepo ) );

          } else {
            System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.NoTransNameSupplied" ) );
          }
        } else {
          System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.CanNotFindSpecifiedDirectory", "" + dirName ) );
        }
      } else {
        System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.NoRepProvided" ) );
      }

    } catch ( Exception e ) {
      getLog().logError( e.getMessage() );
    }

    return null;
  }

  public Trans executeFilesystemBasedCommand( String initialDir, String filename, String jarFilename, String xml ) throws Exception {

    Trans trans = null;

    // Try to load the transformation from file
    if ( !Utils.isEmpty( filename ) ) {

      String filepath = filename;
      // If the filename starts with scheme like zip:, then isAbsolute() will return false even though the
      // the path following the zip is absolute. Check for isAbsolute only if the fileName does not start with scheme
      if ( !KettleVFS.startsWithScheme( filename ) && !FileUtil.isFullyQualified( filename ) ) {
        filepath = initialDir + filename;
      }

      logDebug( "Pan.Log.LoadingTransXML", "" + filepath );
      TransMeta transMeta = new TransMeta( filepath );
      trans = new Trans( transMeta );

    } else if ( !Utils.isEmpty( jarFilename ) ) {

      try {

        logDebug( "Pan.Log.LoadingTransJar", jarFilename );

        InputStream inputStream = PanCommandExecutor.class.getResourceAsStream( jarFilename );
        StringBuilder sbXxml = new StringBuilder();
        int c;
        while ( ( c = inputStream.read() ) != -1 ) {
          sbXxml.append( (char) c );
        }
        inputStream.close();
        Document document = XMLHandler.loadXMLString( sbXxml.toString() );
        TransMeta transMeta = new TransMeta( XMLHandler.getSubNode( document, "transformation" ), null );
        trans = new Trans( transMeta );

      } catch ( Exception e ) {

        System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.ReadingJar", e.toString() ) );
        System.out.println( Const.getStackTracker( e ) );
        throw e;
      }
    } else if ( !Utils.isEmpty( xml ) ) {

      ObjectInputStream ois = null;
      Base64InputStream b64is = null;
      ByteArrayInputStream bis = null;


      try {

        // A) is it a Base64 representation of a serialized Trans object?

        logDebug( "Pan.Log.LoadTransInfo" );
        bis = new ByteArrayInputStream( xml.getBytes() );
        b64is = new Base64InputStream( bis );
        ois = new ObjectInputStream( b64is );

        logDebug( "Pan.Log.AllocateTrans" );

        Transformation t = (Transformation) ois.readObject();
        handleTransformationGraphDeserialization( t );

        logDebug( "Pan.Log.LoadingTransXML", "" + TMP_BASE_DIR + File.separator + t.getId() );
        TransMeta transMeta = new TransMeta( TMP_BASE_DIR + File.separator + t.getId() );
        trans = new Trans( transMeta );
        trans.setMetaStore( getMetaStore() );

      } catch ( Throwable t ) {

        try {

          // B) is it a string representation of the actual KTR xml?
          logDebug( "Pan.Log.LoadTransInfo" );
          bis = new ByteArrayInputStream( xml.getBytes() );
          TransMeta transMeta = new TransMeta( bis, null, true, null, null );

          logDebug( "Pan.Log.AllocateTrans" );
          trans = new Trans( transMeta );
          trans.setMetaStore( getMetaStore() );

        } catch ( Throwable t1 ) {
          System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.LoadingJobEntriesHaltPan" ) );
        }

      } finally {
        IOUtils.closeQuietly( ois );
        IOUtils.closeQuietly( b64is );
        IOUtils.closeQuietly( bis );
      }
    }

    return trans;
  }

  protected void handleTransformationGraphDeserialization( final Transformation t ) throws IOException {

    final String KNOWN_PREFIX = "file://";

    final String TRANS_META = "TransMeta";
    final String SUB_TRANS = "SubTransformations";

    if ( t == null || StringUtils.isEmpty( t.getId() ) || t.getConfig( TRANS_META ) == null ) {
      return; // exit; not much we can do here
    }

    String sanitizedKtrAbsPath = t.getId().startsWith( KNOWN_PREFIX ) ? t.getId().replaceFirst( KNOWN_PREFIX, StringUtils.EMPTY ) : t.getId();
    String ktrAbsPath = TMP_BASE_DIR + File.separator + sanitizedKtrAbsPath;

    Optional<? extends Serializable> subTrans = t.getConfig( SUB_TRANS );

    // store current transformation object
    FileUtils.writeByteArrayToFile( new File( ktrAbsPath ), t.getConfig( TRANS_META ).get().toString().getBytes( Charset.defaultCharset() ) );

    if ( subTrans != null && subTrans.get() != null && !( (Map) subTrans.get() ).isEmpty() ) {

      // recursively call for handleTransformationDeserialization() for each of the sub-transformations
      for ( Map.Entry<String, Transformation> entry : ( (Map<String, Transformation>) subTrans.get() ).entrySet() ) {
        handleTransformationGraphDeserialization( entry.getValue() );
      }
    }
  }


  /**
   * Configures the transformation with the given parameters and their values
   *
   * @param trans        the executable transformation object
   * @param optionParams the list of parameters to set for the transformation
   * @param transMeta    the transformation metadata
   * @throws UnknownParamException
   */
  protected static void configureParameters( Trans trans, NamedParams optionParams,
                                               TransMeta transMeta ) throws UnknownParamException {
    trans.initializeVariablesFrom( null );
    trans.getTransMeta().setInternalKettleVariables( trans );

    // Map the command line named parameters to the actual named parameters.
    // Skip for the moment any extra command line parameter not known in the transformation.
    String[] transParams = trans.listParameters();
    for ( String param : transParams ) {
      String value = optionParams.getParameterValue( param );
      if ( value != null ) {
        trans.setParameterValue( param, value );
        transMeta.setParameterValue( param, value );
      }
    }

    // Put the parameters over the already defined variable space. Parameters get priority.
    trans.activateParameters();
  }

  protected void printTransformationParameters( Trans trans ) throws UnknownParamException {

    if ( trans != null && trans.listParameters() != null ) {

      for ( String pName : trans.listParameters() ) {
        printParameter( pName, trans.getParameterValue( pName ), trans.getParameterDefault( pName ), trans.getParameterDescription( pName ) );
      }
    }
  }

  protected void printRepositoryStoredTransformations( Repository repository, RepositoryDirectoryInterface directory ) throws KettleException {

    logDebug( "Pan.Log.GettingListTransDirectory", "" + directory );
    String[] transformations = repository.getTransformationNames( directory.getObjectId(), false );

    if ( transformations != null ) {
      for ( String trans :  transformations ) {
        System.out.println( trans );
      }
    }
  }

  protected void printRepositories( RepositoriesMeta repositoriesMeta ) {

    if ( repositoriesMeta != null ) {

      logDebug( "Pan.Log.GettingListReps" );

      for ( int i = 0; i < repositoriesMeta.nrRepositories(); i++ ) {
        RepositoryMeta repInfo = repositoriesMeta.getRepository( i );
        System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Log.RepNameDesc", "" + ( i + 1 ),
                repInfo.getName(), repInfo.getDescription() ) );
      }
    }
  }

  private void waitUntilFinished( Trans trans, final long waitMillis ) {

    if ( trans != null && trans.isRunning() ) {

      trans.waitUntilFinished();

      for ( int i = 0; i < 100; i++ ) {
        if ( !trans.isRunning() ) {
          break;
        }

        try {
          Thread.sleep( waitMillis );
        } catch ( Exception e ) {
          break;
        }
      }
    }
  }
}


