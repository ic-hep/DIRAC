# $HeadURL$
__RCSID__ = "$Id$"

import time
import copy
import os.path
import GSI
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.FrameworkSystem.Client.Logger import gLogger

class SocketInfo:

  def __init__( self, infoDict, sslContext = False ):
    self.infoDict = infoDict
    if sslContext:
      self.sslContext = sslContext
    else:
      if self.infoDict[ 'clientMode' ]:
        if 'useCertificates' in self.infoDict and self.infoDict[ 'useCertificates' ]:
          retVal = self.__generateContextWithCerts()
        elif 'proxyString' in self.infoDict:
          retVal = self.__generateContextWithProxyString()
        else:
          retVal = self.__generateContextWithProxy()
      else:
        retVal = self.__generateServerContext()
      if not retVal[ 'OK' ]:
        raise Exception( retVal[ 'Message' ] )

  def __getValue( self, optName, default ):
    if optName not in self.infoDict:
      return default
    return self.infoDict[ optName ]

  def setLocalCredentialsLocation( self, credTuple ):
    self.infoDict[ 'localCredentialsLocation' ] = credTuple

  def getLocalCredentialsLocation( self ):
    return self.infoDict[ 'localCredentialsLocation' ]

  def gatherPeerCredentials( self ):
    certList = self.sslSocket.get_peer_certificate_chain()
    #Servers don't receive the whole chain, the last cert comes alone
    if not self.infoDict[ 'clientMode' ]:
      certList.insert( 0, self.sslSocket.get_peer_certificate() )
    peerChain = X509Chain( certList = certList )
    isProxyChain = peerChain.isProxy()['Value']
    isLimitedProxyChain = peerChain.isLimitedProxy()['Value']
    if isProxyChain:
      identitySubject = peerChain.getIssuerCert()['Value'].getSubjectNameObject()[ 'Value' ]
    else:
      identitySubject = peerChain.getCertInChain( 0 )['Value'].getSubjectNameObject()[ 'Value' ]
    credDict = { 'DN' : identitySubject.one_line(),
                 'CN' : identitySubject.commonName,
                 'x509Chain' : peerChain,
                 'isProxy' : isProxyChain,
                 'isLimitedProxy' : isLimitedProxyChain }
    diracGroup = peerChain.getDIRACGroup()
    if diracGroup[ 'OK' ] and diracGroup[ 'Value' ]:
      credDict[ 'group' ] = diracGroup[ 'Value' ]
    self.infoDict[ 'peerCredentials' ] = credDict
    return credDict

  def setSSLSocket( self, sslSocket ):
    self.sslSocket = sslSocket

  def getSSLSocket( self ):
    return self.sslSocket

  def getSSLContext( self ):
    return self.sslContext

  def clone( self ):
    try:
      return S_OK( SocketInfo( dict( self.infoDict ), self.sslContext ) )
    except Exception, e:
      return S_ERROR( str( e ) )

  def verifyCallback( self, *args, **kwargs ):
    #gLogger.debug( "verify Callback %s" % str( args ) )
    if self.infoDict[ 'clientMode' ]:
      return self._clientCallback( *args, **kwargs )
    else:
      return self._serverCallback( *args, **kwargs )

  def _clientCallback( self, conn, cert, errnum, depth, ok ):
    # This obviously has to be updated
    if depth == 0 and ok == 1:
      hostnameCN = cert.get_subject().commonName
      if hostnameCN in ( self.infoDict[ 'hostname' ], "host/%s" % self.infoDict[ 'hostname' ]  ):
        return 1
      else:
        gLogger.warn( "Server is not who it's supposed to be",
                      "Connecting to %s and it's %s" % ( self.infoDict[ 'hostname' ], hostnameCN ) )
        return ok
    return ok

  def _serverCallback( self, conn, cert, errnum, depth, ok):
    return ok

  def __createContext( self ):
    clientContext = self.__getValue( 'clientMode', False )
    # Initialize context
    if clientContext:
      methodSuffix = "CLIENT_METHOD"
    else:
      methodSuffix = "SERVER_METHOD"
    if 'sslMethod' in self.infoDict:
      methodName = "%s_%s" % ( self.infoDict[ 'sslMethod' ], methodSuffix )
    else:
      methodName = "TLSv1_%s" % ( methodSuffix )
    try:
      method = getattr( GSI.SSL, methodName )
    except:
      return S_ERROR( "SSL method %s is not valid" % self.infoDict[ 'sslMethod' ] )
    self.sslContext = GSI.SSL.Context( method )
    #Enable GSI?
    gsiEnable = False
    if not clientContext or self.__getValue( 'gsiEnable', False ):
      gsiEnable = True
    #DO CA Checks?
    if not self.__getValue( 'skipCACheck', False ):
      #self.sslContext.set_verify( SSL.VERIFY_PEER|SSL.VERIFY_FAIL_IF_NO_PEER_CERT, self.verifyCallback ) # Demand a certificate
      self.sslContext.set_verify( GSI.SSL.VERIFY_PEER|GSI.SSL.VERIFY_FAIL_IF_NO_PEER_CERT, None, gsiEnable ) # Demand a certificate
      casPath = Locations.getCAsLocation()
      if not casPath:
        return S_ERROR( "No valid CAs location found" )
      gLogger.debug( "CAs location is %s" % casPath )
      self.sslContext.load_verify_locations_path( casPath )
    else:
      self.sslContext.set_verify( GSI.SSL.VERIFY_NONE, None, gsiEnable ) # Demand a certificate
    return S_OK()

  def __generateContextWithCerts( self ):
    certKeyTuple = Locations.getHostCertificateAndKeyLocation()
    if not certKeyTuple:
      return S_ERROR( "No valid certificate or key found" )
    self.setLocalCredentialsLocation( certKeyTuple )
    gLogger.debug("Using certificate %s\nUsing key %s" % certKeyTuple )
    retVal = self.__createContext()
    if not retVal[ 'OK' ]:
      return retVal
    #Verify depth to 20 to ensure accepting proxies of proxies of proxies....
    self.sslContext.set_verify_depth( 50 )
    self.sslContext.use_certificate_chain_file( certKeyTuple[0] )
    self.sslContext.use_privatekey_file(  certKeyTuple[1] )
    return S_OK()

  def __generateContextWithProxy( self ):
    if 'proxyLocation' in self.infoDict:
      proxyPath = self.infoDict[ 'proxyLocation' ]
      if not os.path.isfile( proxyPath ):
        return S_ERROR( "Defined proxy is not a file" )
    else:
      proxyPath = Locations.getProxyLocation()
      if not proxyPath:
        return S_ERROR( "No valid proxy found" )
    self.setLocalCredentialsLocation( ( proxyPath, proxyPath ) )
    gLogger.debug( "Using proxy %s" % proxyPath )
    retVal = self.__createContext()
    if not retVal[ 'OK' ]:
      return retVal
    self.sslContext.use_certificate_chain_file( proxyPath )
    self.sslContext.use_privatekey_file( proxyPath )
    return S_OK()

  def __generateContextWithProxyString( self ):
    proxyString = self.infoDict[ 'proxyString' ]
    self.setLocalCredentialsLocation( ( proxyString, proxyString ) )
    gLogger.debug( "Using string proxy" )
    retVal = self.__createContext()
    if not retVal[ 'OK' ]:
      return retVal
    self.sslContext.use_certificate_chain_string( proxyString )
    self.sslContext.use_privatekey_string( proxyString )
    return S_OK()

  def __generateServerContext( self ):
    retVal = self.__generateContextWithCerts()
    if not retVal[ 'OK' ]:
      return retVal
    self.sslContext.set_session_id( "DISETConnection%s" % str( time.time() ) )
    #self.sslContext.get_cert_store().set_flags( GSI.crypto.X509_CRL_CHECK )
    if 'SSLSessionTimeout' in self.infoDict:
      timeout = int( self.infoDict['SSLSessionTimeout'] )
      gLogger.debug( "Setting session timeout to %s" % timeout )
      self.sslContext.set_session_timeout( timeout )
    return S_OK()

  def doClientHandshake( self ):
    self.sslSocket.set_connect_state()
    return self.__sslHandshake()

  def doServerHandshake( self ):
    self.sslSocket.set_accept_state()
    return self.__sslHandshake()

  #@gSynchro
  def __sslHandshake( self ):
    start = time.time()
    timeout = self.infoDict[ 'timeout' ]
    while True:
      if timeout:
        if time.time() - start > timeout:
          return S_ERROR( "Handshake timeout exceeded" )
      try:
        self.sslSocket.do_handshake()
        break
      except GSI.SSL.WantReadError:
        time.sleep( 0.1 )
      except GSI.SSL.WantWriteError:
        time.sleep( 0.1 )
      except GSI.SSL.Error, v:
        #gLogger.warn( "Error while handshaking", "\n".join( [ stError[2] for stError in v.args[0] ] ) )
        gLogger.warn( "Error while handshaking", v )
        return S_ERROR( "Error while handshaking" )
      except Exception, v:
        gLogger.warn( "Error while handshaking", v )
        return S_ERROR( "Error while handshaking" )
    credentialsDict = self.gatherPeerCredentials()
    if self.infoDict[ 'clientMode' ]:
      hostnameCN = credentialsDict[ 'CN' ]
      if hostnameCN.split("/")[-1] != self.infoDict[ 'hostname' ]:
        gLogger.warn( "Server is not who it's supposed to be",
                      "Connecting to %s and it's %s" % ( self.infoDict[ 'hostname' ], hostnameCN ) )
    gLogger.debug( "", "Authenticated peer (%s)" % credentialsDict[ 'DN' ] )
    return S_OK( credentialsDict )
