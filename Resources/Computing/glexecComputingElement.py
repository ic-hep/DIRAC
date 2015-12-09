""" A computing element class that attempts to use glexec if available then
    defaults to the standard InProcess Computing Element behaviour.
"""

__RCSID__ = "$Id$"

import os
import re
import stat
import uuid
import distutils.spawn
from tempfile import mkdtemp, NamedTemporaryFile
from shutil import rmtree

import DIRAC

from DIRAC                                                  import S_OK, S_ERROR, gConfig

from DIRAC.Resources.Computing.ComputingElement             import ComputingElement
from DIRAC.Core.Utilities.ThreadScheduler                   import gThreadScheduler
from DIRAC.Core.Utilities.Subprocess                        import shellCall



MandatoryParameters = [ ]

def S_RETRYERROR(computing_element, *args, **kwargs):
    ret = S_ERROR(*args, **kwargs)
    if computing_element.ceParameters.get('RescheduleOnError', False):
      ret['ReschedulePayload'] = True
    return ret

class glexecComputingElement( ComputingElement ):

  mandatoryParameters = MandatoryParameters

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )
    self.submittedJobs = 0

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )
    # Now glexec specific ones

  #############################################################################
  def submitJob( self, executableFile, proxy, dummy = None ):
    """ Method to submit job, should be overridden in sub-class.
    """
    self.log.notice( 'Starting glexec submission, reschedule on error: %s' % \
                        self.ceParameters.get('RescheduleOnError', False))
    
    self.log.verbose( 'Setting up proxy for payload' )
    result = self.writeProxyToFile( proxy )
    if not result['OK']:
      return result

    payloadProxy = result['Value']
    if not os.environ.has_key( 'X509_USER_PROXY' ):
      self.log.error( 'X509_USER_PROXY variable for pilot proxy not found in local environment' )
      return S_RETRYERROR(self, 'X509_USER_PROXY not found' )

    pilotProxy = os.environ['X509_USER_PROXY']
    self.log.info( 'Pilot proxy X509_USER_PROXY=%s' % pilotProxy )
    os.environ[ 'GLEXEC_CLIENT_CERT' ] = payloadProxy
    os.environ[ 'GLEXEC_SOURCE_PROXY' ] = payloadProxy
    self.log.info( '\n'.join( [ 'Set payload proxy variables:',
                                'GLEXEC_CLIENT_CERT=%s' % payloadProxy,
                                'GLEXEC_SOURCE_PROXY=%s' % payloadProxy ] ) )

    #Determine glexec location (default to standard InProcess behaviour if not found)
    glexecLocation = None
    result = self.glexecLocate()
    if result['OK']:
      glexecLocation = result['Value']
      self.log.notice( 'glexec found for local site at %s' % glexecLocation )

    if glexecLocation:
      result = self.recursivelyChangePermissions()
      if not result['OK']:
        self.log.error( 'Permissions change failed, continuing regardless...' )
    else:
      self.log.error( 'glexec not found' )
      return S_RETRYERROR(self, "Could not find glexec")

    debug_file = os.path.join(os.path.dirname(__file__), 'debug_script.py')
    self.log.notice("Checking for debug script: %s" % debug_file)
    if os.path.isFile(debug_file):
        self.log.notice("Executing %s ..." % debug_file)
        execfile(debug_file)
    else:
        self.log.notice("No debug script found.")

    # Determine if the running directory is executable by the user, if not then run in home
    pilot_dir = os.getcwd()
    job_dir = os.path.join('~', 'job_%s' % uuid.uuid4())
    running_in_home = False
    path = pilot_dir
    while path != '/':  # we will run in the home dir if no route to the pilot_dir
      if not os.stat(path).st_mode & stat.S_IXOTH:
        self.log.notice("Switching to home dir as current path '%s' contains element '%s' which is not executable by other users" % (pilot_dir, path))
        running_in_home = True
        result = self.glexecMakeDir(glexecLocation, job_dir)
        if not result['OK']:
            self.log.error("Failed to create job dir")  
            return S_RETRYERROR(self, "couldn't setup the user job dir inside their home dir")
        break
      path = os.path.dirname(path)

    # if running dir is executable by user then create a Dirac install dir and job dir
    if not running_in_home:
      result = self.setupPilotDir(glexecLocation, pilot_dir)
      if not result['OK']:
        self.log.error("Submission aborted as no pilot and job dir setup.")
        return result
      pilot_dir, job_dir = result['Value']

    # Ensure that the proxy target location is known to the pilot and will be the same each time
    # glexec pulls it through. Without this glexec adds 6 random chars to the end each time so we don't
    # renew the same proxy.
    with NamedTemporaryFile(prefix='x509up_glexec_') as tmp:
        os.environ['GLEXEC_TARGET_PROXY'] = os.path.join(job_dir, os.path.basename(tmp.name))

    #Test glexec with payload proxy prior to submitting the job
    self.log.notice("Running in job_dir: %s"% job_dir)
    result = self.glexecTest( glexecLocation, job_dir )
    if not result['OK']:
      res = self.analyseExitCode( result['Value'] ) #take no action as we currently default to InProcess
      self.log.error( 'glexec test failed...' )
      return S_RETRYERROR(self, 'gLexec Test Failed: %s' % res['Value'] )

    #Revert to InProcess behaviour
    if not glexecLocation:
      self.log.info( 'glexec is not found, setting X509_USER_PROXY for payload proxy' )
      os.environ[ 'X509_USER_PROXY' ] = payloadProxy

    self.log.verbose( 'Starting process for monitoring payload proxy' )
    gThreadScheduler.addPeriodicTask( self.proxyCheckPeriod, self.monitorProxy,
                                      taskArgs = ( glexecLocation, pilotProxy, payloadProxy ),
                                      executions = 0, elapsedTime = 0 )

    # read in old exe file and make necessary path modifications
    old_file = ''
    with open(executableFile, 'rb') as exec_file:
      for line in re.findall(r'^(?![\s]*$)([^#\n]*).*$', exec_file.read(), flags=re.MULTILINE):
        match = re.match(r'^[^ ]*/*python[\d.]* +([^ ]*/job/Wrapper/Wrapper_[\d.]+) ', line)
        if match is not None:
          wrapper_file = match.groups()[0]
          self.glexecCopyFile(glexecLocation,
                              infile=wrapper_file,
                              outfile=os.path.join(job_dir, os.path.basename(wrapper_file)))
        old_file += re.sub(r'^[^ ]*/*python[\d.]* +[^ ]*/job/Wrapper/(Wrapper_[\d.]+) ', r'python \1 ', line) + '\n'

    # write the new exe script
    glexec_executableFile = os.path.join(os.path.dirname(executableFile),
                                     "glexec_%s" % os.path.basename(executableFile))
    with open(glexec_executableFile, 'wb') as new_file:
      new_script = \
"""#!/bin/bash
set -e
cd {jobDir!s}
export PATH=/usr/bin:/bin:/usr/sbin:/sbin
python dirac-pilot.py -S {diracSetup!s} -r {diracVersion!s} -X InstallDIRAC,ConfigureBasics -C {diracConfigServer!s}
source bashrc
dirac-proxy-info
{oldFile!s}
""".format(jobDir=job_dir,
           diracSetup=gConfig.getValue( 'DIRAC/Setup' ),
           diracVersion=DIRAC.version,
           diracConfigServer=re.sub(' *, *', ',', gConfig.getValue('/DIRAC/Configuration/Servers').strip(', ')),
           oldFile=old_file)
      self.log.debug("Using script:\n %s" % new_script)
      new_file.write(new_script)

    #Submit job
    self.log.info( 'Changing permissions of executable to 0755' )
    try:
      os.chmod( os.path.abspath( glexec_executableFile ), stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
    except Exception, x:
      self.log.error( 'Failed to change permissions of executable to 0755 with exception', 
                      '\n%s' % ( x ) )

    # push files through to the user job dir
    self.glexecCopyFile(glexecLocation,
                        infile=os.path.abspath(glexec_executableFile),
                        outfile=os.path.join(job_dir, os.path.basename(glexec_executableFile)),
                        executable=True)
    self.glexecCopyFile(glexecLocation,
                        infile=os.path.join(DIRAC.rootPath, 'DIRAC/Core/scripts/dirac-install.py'),
                        outfile=os.path.join(job_dir, 'dirac-install.py'))
    self.glexecCopyFile(glexecLocation,
                        infile=os.path.join(DIRAC.rootPath, 'DIRAC/WorkloadManagementSystem/PilotAgent/dirac-pilot.py'),
                        outfile=os.path.join(job_dir, 'dirac-pilot.py'))
    self.glexecCopyFile(glexecLocation,
                        infile=os.path.join(DIRAC.rootPath, 'DIRAC/WorkloadManagementSystem/PilotAgent/pilotTools.py'),
                        outfile=os.path.join(job_dir, 'pilotTools.py'))
    self.glexecCopyFile(glexecLocation,
                        infile=os.path.join(DIRAC.rootPath, 'DIRAC/WorkloadManagementSystem/PilotAgent/pilotCommands.py'),
                        outfile=os.path.join(job_dir, 'pilotCommands.py'))

    result = self.glexecExecute(os.path.join(job_dir, os.path.basename(glexec_executableFile)),
                                glexecLocation )
    if not result['OK']:
      self.analyseExitCode( result['Value'] ) #take no action as we currently default to InProcess
      self.log.error( 'Failed glexecExecute', result )
      return result

    if not running_in_home:
      result = self.removePilotDir(glexecLocation, pilot_dir, job_dir)
      if not result['OK']:
        self.log.error("Failed to clean up the job dir: %s" % job_dir)

    self.log.debug( 'glexec CE result OK' )
    self.submittedJobs += 1
    return S_OK()

  #############################################################################
  def recursivelyChangePermissions( self ):
    """ Ensure that the current directory and all those beneath have the correct
        permissions.
    """
    currentDir = os.getcwd()
    try:
      self.log.info( 'Trying to explicitly change permissions for parent directory %s' % currentDir )
      os.chmod( currentDir, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
    except Exception, x:
      self.log.error( 'Problem changing directory permissions in parent directory', str( x ) )

    return S_OK()

    userID = None

    res = shellCall( 10, 'ls -al' )
    if res['OK'] and res['Value'][0] == 0:
      self.log.info( 'Contents of the working directory before permissions change:' )
      self.log.info( str( res['Value'][1] ) )
    else:
      self.log.error( 'Failed to list the log directory contents', str( res['Value'][2] ) )

    res = shellCall( 10, 'id -u' )
    if res['OK'] and res['Value'][0] == 0:
      userID = res['Value'][1]
      self.log.info( 'Current user ID is: %s' % ( userID ) )
    else:
      self.log.error( 'Failed to obtain current user ID', str( res['Value'][2] ) )
      return res

    res = shellCall( 10, 'ls -al %s/../' % currentDir )
    if res['OK'] and res['Value'][0] == 0:
      self.log.info( 'Contents of the parent directory before permissions change:' )
      self.log.info( str( res['Value'][1] ) )
    else:
      self.log.error( 'Failed to list the parent directory contents', str( res['Value'][2] ) )

    self.log.verbose( 'Changing permissions to 0755 in current directory %s' % currentDir )
    for dirName, _, files in os.walk( currentDir ):
      try:
        self.log.info( 'Changing file and directory permissions to 0755 for %s' % dirName )
        if os.stat( dirName )[4] == userID and not os.path.islink( dirName ):
          os.chmod( dirName, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
        for toChange in files:
          toChange = os.path.join( dirName, toChange )
          if os.stat( toChange )[4] == userID and not os.path.islink( toChange ):
            os.chmod( toChange, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
      except Exception, x:
        self.log.error( 'Problem changing directory permissions', str( x ) )

    self.log.info( 'Permissions in current directory %s updated successfully' % ( currentDir ) )
    res = shellCall( 10, 'ls -al' )
    if res['OK'] and res['Value'][0] == 0:
      self.log.info( 'Contents of the working directory after changing permissions:' )
      self.log.info( str( res['Value'][1] ) )
    else:
      self.log.error( 'Failed to list the log directory contents', str( res['Value'][2] ) )

    res = shellCall( 10, 'ls -al %s/../' % currentDir )
    if res['OK'] and res['Value'][0] == 0:
      self.log.info( 'Contents of the parent directory after permissions change:' )
      self.log.info( str( res['Value'][1] ) )
    else:
      self.log.error( 'Failed to list the parent directory contents', str( res['Value'][2] ) )

    return S_OK()

  #############################################################################
  def analyseExitCode( self, resultTuple ):
    """ Analyses the exit codes in case of glexec failures.  The convention for
        glexec exit codes is listed below:

          Shell exit codes:
          127 - command not found
          129 - command died due to signal 1 (SIGHUP)
          130 - command died due to signal 2 (SIGINT)

          glexec specific codes:
          201 - client error
          202 - internal error
          203 - authz error
    """
    if not resultTuple:
      return S_OK()

    # FIXME: the wrapper will return:
    #   > 0 if there are problems with the payload
    #   < 0 if there are problems with the wrapper itself
    #   0 if everything is OK

    codes = {}
    codes[127] = 'Shell exited, command not found'
    codes[129] = 'Shell interrupt signal 1 (SIGHUP)'
    codes[130] = 'Shell interrupt signal 2 (SIGINT)'
    codes[201] = 'glexec failed with client error'
    codes[202] = 'glexec failed with internal error'
    codes[203] = 'glexec failed with authorization error'

    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info( 'glexec call failed with status %s' % ( status ) )
    self.log.info( 'glexec stdout:\n%s' % stdOutput )
    self.log.info( 'glexec stderr:\n%s' % stdError )

    error = None
    for code, msg in codes.items():
      self.log.verbose( 'Exit code %s => %s' % ( code, msg ) )
      if status == code:
        error = msg

    if not error:
      self.log.error( 'glexec exit code not in expected list', '%s' % status )
    else:
      self.log.error( 'Error in glexec return code', '%s = %s' % ( status, error ) )

    return S_OK( error )

  #############################################################################
  def glexecTest( self, glexecLocation, job_dir='~'):
    """Ensure that the current DIRAC distribution is group readable e.g. dirac-proxy-info
       also check the status code of the glexec call.
    """
    if not glexecLocation:
      return S_OK( 'Nothing to test' )

    testFile = 'glexecTest.sh'
    cmds = ['#!/bin/sh']
    cmds.append( 'id' )
    cmds.append( 'hostname' )
    cmds.append( 'date' )
    fopen = open( testFile, 'w' )
    fopen.write( '\n'.join( cmds ) )
    fopen.close()
    self.log.info( 'Changing permissions of test script to 0755' )
    try:
      os.chmod( os.path.abspath( testFile ), stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
    except Exception, x:
      self.log.error( 'Failed to change permissions of test script to 0755 with exception', 
                      '\n%s' % ( x ) )
      return S_RETRYERROR(self, 'Could not change permissions of test script' )

    glexec_executableFile = os.path.join(job_dir, testFile)
    self.glexecCopyFile(glexecLocation,
                        infile=os.path.abspath(testFile),
                        outfile=glexec_executableFile,
                        executable=True)

    return self.glexecExecute( glexec_executableFile, glexecLocation )

  #############################################################################
  def setupPilotDir(self, glexecLocation, pilot_dir):
    """Setup a temporary pilot dir with a job dir inside for the dirac install"""
    if not glexecLocation:
      return S_RETRYERROR(self, "Can't setup pilot dir as can't find glexec")
    pilot_dir = mkdtemp(prefix='glexec_pilot', dir=pilot_dir)
    job_dir = os.path.join(pilot_dir, 'job_dir_glexec')
    os.chdir(pilot_dir)
    os.chmod(pilot_dir, 0o1777)  # sticky bit stops the inner dir being overwritten by another user before we chmod again
    result = self.glexecMakeDir(glexecLocation, job_dir)
    os.chmod(pilot_dir, 0o0711)
    if not result['OK']:
      self.log.error("Failed to create job dir as user")
      return S_RETRYERROR(self, "Failed to create job dir with user credentials")
    return S_OK((pilot_dir, job_dir))

  #############################################################################
  def removePilotDir(self, glexecLocation, pilot_dir, job_dir):
    """Remove the directory structures created in setupPilotDir"""
    os.chmod(pilot_dir, 0o1777)
    cmd = "%s /bin/bash -c '/bin/rm -rf %s'" % (glexecLocation, job_dir)
    result = shellCall( 0, cmd, callbackFunction = self.sendOutput )
    os.chmod(pilot_dir, 0o0711)
    if not result['OK']:
      self.log.error("Failed to call: glexec /bin/rm")
      return S_RETRYERROR(self, "Failed to delete job dir")

    try:
      rmtree(pilot_dir)
    except Exception, x:
      self.log.error("Failed to clean up pilot dir: %s" % pilot_dir)
      return S_RETRYERROR(self, "Failed to remove the pilot dir")

    return S_OK()

  #############################################################################
  def glexecMakeDir(self, glexecLocation, path):
    """Create new directories with the users credentials"""
    self.log.notice("using glexec to make: %s" % path)
    cmd = "%s /bin/bash -c '/bin/mkdir -p %s'" % (glexecLocation, path)
    result = shellCall( 0, cmd, callbackFunction = self.sendOutput )
    if not result['OK']:
      self.log.error("Failed to create dir: %s with '%s'" % (path, cmd))
      return result
    return S_OK()

  #############################################################################
  def glexecCopyFile(self, glexecLocation, infile, outfile=None, executable=False):
    """Copy files from one location with the pilots credentials to another with the users credentials"""
    executemod = ''
    if executable:
      executemod = "; chmod 700 %s" % outfile
    if outfile is None:
      outfile = os.path.basename(infile)
    cmd = "/bin/cat %s | %s /bin/bash -c '/bin/cat > %s%s'" % (infile, glexecLocation, outfile, executemod)    
    result = shellCall( 0, cmd, callbackFunction = self.sendOutput )
    if not result['OK']:
      self.log.error("Failed to copy file through glexec")
      return S_RETRYERROR(self, "Failed to copy file")
    return S_OK()

  #############################################################################
  def glexecExecute( self, executableFile, glexecLocation ):
    """Run glexec with checking of the exit status code.
    """
    cmd = executableFile
    if glexecLocation and executableFile:
      cmd = "%s /bin/bash -lc '%s'" % ( glexecLocation, executableFile )
    if glexecLocation and not executableFile:
      cmd = '%s /bin/true' % ( glexecLocation )

    self.log.info( 'CE submission command is: %s' % cmd )
    result = shellCall( 0, cmd, callbackFunction = self.sendOutput )
    if not result['OK']:
      result['Value'] = ( 0, '', '' )
      return result

    resultTuple = result['Value']
    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]
    self.log.info( "Status after the glexec execution is %s" % str( status ) )
    if status >=127:
      error = S_RETRYERROR(self, status )
      error['Value'] = ( status, stdOutput, stdError )
      return error

    return result

  #############################################################################
  def glexecLocate( self ):
    """Try to find glexec on the local system, if not found default to InProcess.
    """
    glexecPath = ""
    if os.environ.has_key( 'OSG_GLEXEC_LOCATION' ):
      glexecPath = '%s' % ( os.environ['OSG_GLEXEC_LOCATION'] )
    elif os.environ.has_key( 'GLITE_LOCATION' ):
      glexecPath = '%s/sbin/glexec' % ( os.environ['GLITE_LOCATION'] )
    else: #try to locate the excutable in the PATH
      glexecPath = distutils.spawn.find_executable( "glexec" )
    if not glexecPath:
      self.log.error( 'Unable to locate glexec, site does not have GLITE_LOCATION nor OSG_GLEXEC_LOCATION defined' )
      return S_RETRYERROR(self, 'glexec not found' )

    if not os.path.exists( glexecPath ):
      self.log.error( 'glexec not found at path %s' % ( glexecPath ) )
      return S_RETRYERROR(self, 'glexec not found' )

    return S_OK( glexecPath )

  #############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return result

  #############################################################################
  def monitorProxy( self, glexecLocation, pilotProxy, payloadProxy ):
    """ Monitor the payload proxy and renew as necessary.
    """
    self.log.notice("Monitoring proxy.")
    retVal = self._monitorProxy( pilotProxy, payloadProxy )
    if not retVal['OK']:
      # Failed to renew the proxy, nothing else to be done
      self.log.error("Failed to renew the proxy.")
      return retVal

    if not retVal['Value']:
      # No need to renew the proxy, nothing else to be done
      self.log.notice("Proxy doesn't need renewing")
      return retVal

    if glexecLocation:
      self.log.notice( 'Rerunning glexec without arguments to renew payload proxy' )
      result = self.glexecExecute( None, glexecLocation )
      if not result['OK']:
        self.log.error( 'Failed glexecExecute', result )
    else:
      self.log.notice( 'Running without glexec, checking local proxy' )

    return S_OK( 'Proxy checked' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
