########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/DB/ProxyDB.py,v 1.7 2008/07/07 17:19:01 acasajus Exp $
########################################################################
""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id: ProxyDB.py,v 1.7 2008/07/07 17:19:01 acasajus Exp $"

import time
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security.X509Request import X509Request
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security.MyProxy import MyProxy
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security import CS

class ProxyDB(DB):

  def __init__(self, requireVoms = False,
               useMyProxy = False,
               MyProxyServer = False,
               maxQueueSize = 10 ):
    DB.__init__(self,'ProxyDB','Framework/ProxyDB',maxQueueSize)
    self.__defaultRequestLifetime = 300 # 5min
    self.__vomsRequired = requireVoms
    self.__useMyProxy = useMyProxy
    self.__MyProxyServer = MyProxyServer
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ])

  def __initializeDB(self):
    """
    Create the tables
    """
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesD = {}

    if 'ProxyDB_Requests' not in tablesInDB:
      tablesD[ 'ProxyDB_Requests' ] = { 'Fields' : { 'Id' : 'INTEGER AUTO_INCREMENT NOT NULL',
                                                     'UserDN' : 'VARCHAR(255) NOT NULL',
                                                     'UserGroup' : 'VARCHAR(255) NOT NULL',
                                                     'Pem' : 'BLOB',
                                                     'ExpirationTime' : 'DATETIME'
                                                   },
                                        'PrimaryKey' : 'Id'
                                      }
    if 'ProxyDB_Proxies' not in tablesInDB:
      tablesD[ 'ProxyDB_Proxies' ] = { 'Fields' : { 'UserDN' : 'VARCHAR(255) NOT NULL',
                                                    'UserGroup' : 'VARCHAR(255) NOT NULL',
                                                    'Pem' : 'BLOB',
                                                    'ExpirationTime' : 'DATETIME',
                                                    'PersistentFlag' : 'ENUM ("True","False") NOT NULL DEFAULT "True"',
                                                  },
                                      'PrimaryKey' : [ 'UserDN', 'UserGroup' ]
                                     }
    if 'ProxyDB_Log' not in tablesInDB:
      tablesD[ 'ProxyDB_Log' ] = { 'Fields' : { 'IssuerDN' : 'VARCHAR(255) NOT NULL',
                                                'IssuerGroup' : 'VARCHAR(255) NOT NULL',
                                                'TargetDN' : 'VARCHAR(255) NOT NULL',
                                                'TargetGroup' : 'VARCHAR(255) NOT NULL',
                                                'Action' : 'VARCHAR(128) NOT NULL',
                                                'Timestamp' : 'DATETIME',
                                              }
                                  }
    return self._createTables( tablesD )

  def generateDelegationRequest( self, proxyChain, userDN, userGroup ):
    """
    Generate a request  and store it for a given proxy Chain
    """
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return retVal
    connObj = retVal[ 'Value' ]
    retVal = proxyChain.generateProxyRequest()
    if not retVal[ 'OK' ]:
      return retVal
    request = retVal[ 'Value' ]
    retVal = request.dumpRequest()
    if not retVal[ 'OK' ]:
      return retVal
    reqStr = retVal[ 'Value' ]
    retVal = request.dumpPKey()
    if not retVal[ 'OK' ]:
      return retVal
    allStr = reqStr + retVal[ 'Value' ]
    cmd = "INSERT INTO `ProxyDB_Requests` ( Id, UserDN, UserGroup, Pem, ExpirationTime )"
    cmd += " VALUES ( 0, '%s', '%s', '%s', TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ) )" % ( userDN,
                                                                              userGroup,
                                                                              allStr,
                                                                              self.__defaultRequestLifetime )
    retVal = self._update( cmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    #99% of the times we will stop here
    if 'lastRowId' in retVal:
      return S_OK( { 'id' : retVal['lastRowId'], 'request' : reqStr } )
    #If the lastRowId hack does not work. Get it by hand
    retVal = self._query( "SELECT Id FROM `ProxyDB_Requests` WHERE Pem='%s'" % reqStr )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    if len( data ) == 0:
      return S_ERROR( "Insertion of the request in the db didn't work as expected" )
    #Here we go!
    return S_OK( { 'id' : data[0][0], 'request' : reqStr } )

  def retrieveDelegationRequest( self, requestId, userDN, userGroup ):
    """
    Retrieve a request from the DB
    """
    cmd = "SELECT Pem FROM `ProxyDB_Requests` WHERE Id = %s AND UserDN = '%s' and UserGroup = '%s'" % ( requestId,
                                                                                                userDN,
                                                                                                userGroup )
    retVal = self._query( cmd)
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    if len( data ) == 0:
      return S_ERROR( "No requests with id %s" % requestId )
    request = X509Request()
    retVal = request.loadAllFromString( data[0][0] )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( request )

  def purgeExpiredRequests( self ):
    """
    Purge expired requests from the db
    """
    cmd = "DELETE FROM `ProxyDB_Requests` WHERE ExpirationTime < UTC_TIMESTAMP()"
    return self._update( cmd )

  def deleteRequest( self, requestId ):
    """
    Delete a request from the db
    """
    cmd = "DELETE FROM `ProxyDB_Requests` WHERE Id=%s" % requestId
    return self._update( cmd )

  def __checkVOMSisAlignedWithGroup( self, userGroup, chain ):
    voms = VOMS()
    if not voms.vomsInfoAvailable():
      if self.__vomsRequired:
        return S_ERROR( "VOMS is required, but it's not available" )
      gLogger.warn( "voms-proxy-info is not available" )
      return S_OK()
    retVal = voms.getVOMSAttributes( chain )
    if not retVal[ 'OK' ]:
      return retVal
    attr = retVal[ 'Value' ]
    validVOMSAttrs = CS.getVOMSAttributeForGroup( userGroup )
    if len( attr ) == 0 or attr[0] in validVOMSAttrs:
      return S_OK( 'OK' )
    msg = "VOMS attributes are not aligned with dirac group"
    msg += "Attributes are %s and allowed are %s for group %s" % ( attr, validVOMSAttrs, userGroup )
    return S_ERROR( msg )

  def completeDelegation( self, requestId, userDN, userGroup, delegatedPem ):
    """
    Complete a delegation and store it in the db
    """
    retVal = self.retrieveDelegationRequest( requestId, userDN, userGroup )
    if not retVal[ 'OK' ]:
      return retVal
    request = retVal[ 'Value' ]
    chain = X509Chain( keyObj = request.getPKey() )
    retVal = chain.loadChainFromString( delegatedPem )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = chain.isValidProxy()
    if not retVal[ 'OK' ]:
      return retVal
    if not retVal[ 'Value' ]:
      return S_ERROR( "Chain received is not a valid proxy: %s" % retVal[ 'Message' ] )

    retVal = request.checkChain( chain )
    if not retVal[ 'OK' ]:
      return retVal
    if not retVal[ 'Value' ]:
      return S_ERROR( "Received chain does not match request: %s" % retVal[ 'Message' ] )

    retVal = self.__checkVOMSisAlignedWithGroup(userGroup, chain )
    if not retVal[ 'OK' ]:
      return retVal

    retVal = self.storeProxy( userDN, userGroup, chain )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self.deleteRequest( requestId )
    if not retVal[ 'OK' ]:
      return retVal
    self.logAction( "upload proxy", userDN, userGroup, userDN, userGroup )
    return S_OK()

  def storeProxy(self, userDN, userGroup, chain ):
    """ Store user proxy into the Proxy repository for a user specified by his
        DN and group.
    """
    #Get remaining secs
    retVal = chain.getRemainingSecs()
    if not retVal[ 'OK' ]:
      return retVal
    remainingSecs = retVal[ 'Value' ]
    #Compare the DNs
    retVal = chain.getIssuerCert()
    if not retVal[ 'OK' ]:
      return retVal
    proxyIdentityDN = retVal[ 'Value' ].getSubjectDN()[ 'Value' ]
    if not userDN == proxyIdentityDN:
      msg = "Mismatch in the user DN"
      vMsg = "Proxy says %s and credentials are %s" % ( proxyIdentityDN, userDN )
      gLogger.error( msg, vMsg )
      return S_ERROR(  "%s. %s" % ( msg, vMsg ) )
    #Check the groups
    retVal = chain.getDIRACGroup()
    if not retVal[ 'OK' ]:
      return retVal
    proxyGroup = retVal[ 'Value' ]
    if not proxyGroup:
      proxyGroup = CS.getDefaultUserGroup()
    if not userGroup == proxyGroup:
      msg = "Mismatch in the user group"
      vMsg = "Proxy says %s and credentials are %s" % ( proxyGroup, userGroup )
      gLogger.error( msg, vMsg )
      return S_ERROR(  "%s. %s" % ( msg, vMsg ) )
    #Check if its limited
    if chain.isLimitedProxy()['Value']:
      return S_ERROR( "Limited proxies are not allowed to be stored" )
    gLogger.info( "Storing proxy for credentials %s (%s secs)" %( proxyIdentityDN,remainingSecs ) )

    # Check what we have already got in the repository
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ), Pem FROM `ProxyDB_Proxies` WHERE UserDN='%s' AND UserGroup='%s'" % ( userDN,
                                                                                                               userGroup)
    result = self._query( cmd )
    if not result['OK']:
      return result
    # check if there is a previous ticket for the DN
    data = result[ 'Value' ]
    sqlInsert = True
    if len( data ) > 0:
      sqlInsert = False
      pem = data[0][1]
      if pem:
        remainingSecsInDB = data[0][0]
        if remainingSecs <= remainingSecsInDB:
          gLogger.info( "Proxy stored is longer than uploaded, omitting.", "%s in uploaded, %s in db" % (remainingSecs, remainingSecsInDB ) )
          return S_OK()

    pemChain = chain.dumpAllToString()['Value']
    if sqlInsert:
      cmd = "INSERT INTO `ProxyDB_Proxies` ( UserDN, UserGroup, Pem, ExpirationTime, PersistentFlag ) VALUES "
      cmd += "( '%s', '%s', '%s', TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ), 'False' )" % ( userDN,
                                                                                  userGroup,
                                                                                  pemChain,
                                                                                  remainingSecs )
    else:
      cmd = "UPDATE `ProxyDB_Proxies` set Pem='%s', ExpirationTime = TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ) WHERE UserDN='%s' AND UserGroup='%s'" % ( pemChain,
                                                                                                                                                remainingSecs,
                                                                                                                                                userDN,
                                                                                                                                                userGroup)

    return self._update( cmd )

  def purgeExpiredProxies( self ):
    """
    Purge expired requests from the db
    """
    cmd = "DELETE FROM `ProxyDB_Proxies` WHERE ExpirationTime < UTC_TIMESTAMP() and PersistentFlag = 'False'"
    return self._update( cmd )

  def deleteProxy( self, userDN, userGroup ):
    """ Remove proxy of the given user from the repository
    """

    req = "DELETE FROM `ProxyDB_Proxies` WHERE UserDN='%s' AND UserGroup='%s'" % ( userDN,
                                                                                   userGroup )
    return self._update(req)

  def __getPemAndTimeLeft( self, userDN, userGroup ):
    cmd = "SELECT Pem, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) from `ProxyDB_Proxies`"
    cmd += "WHERE UserDN='%s' AND UserGroup = '%s' AND TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > 0" % ( userDN, userGroup )
    retVal = self._query(cmd)
    if not retVal['OK']:
      return retVal
    data = retVal[ 'Value' ]
    if len( data ) == 0 or not data[0][0]:
      return S_ERROR( "%s@%s has no proxy registered" % ( userDN, userGroup ) )
    return S_OK( ( data[0][0], data[0][1] ) )

  def renewFromMyProxy( self, userDN, userGroup, lifeTime = False, chain = False ):
    if not lifeTime:
      lifeTime = 43200
    if not self.__useMyProxy:
      return S_ERROR( "myproxy is disabled" )
    #Get the chain
    if not chain:
      retVal = self.__getPemAndTimeLeft( userDN, userGroup )
      if not retVal[ 'OK' ]:
        return retVal
      pemData = retVal[ 'Value' ][0]
      chain = X509Chain()
      retVal = chain.loadProxyFromString( pemData )
      if not retVal[ 'OK' ]:
        return retVal

    myProxy = MyProxy( server = self.__MyProxyServer )
    retVal = myProxy.getDelegatedProxy( chain, lifeTime )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = chain.getDIRACGroup()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't retrieve DIRAC Group from renewed proxy: %s" % retVal[ 'Message' ] )
    chainGroup = retVal['Value']
    if chainGroup != userGroup:
      return S_ERROR( "Mismatch between renewed proxy group and expected: %s vs %s" % ( userGroup, chainGroup ) )
    self.storeProxy( userDN, userGroup, chain )
    retVal = myProxy.getServiceDN()
    if not retVal[ 'OK' ]:
      hostDN = userDN
    else:
      hostDN = retVal[ 'Value' ]
    self.logAction( "myproxy renewal", hostDN, "host", userDN, userGroup )
    return S_OK( chain )

  def getProxy( self, userDN, userGroup, requiredLifeTime = False ):
    """ Get proxy string from the Proxy Repository for use with userDN
        in the userGroup
    """

    retVal = self.__getPemAndTimeLeft( userDN, userGroup )
    if not retVal[ 'OK' ]:
      return retVal
    pemData = retVal[ 'Value' ][0]
    timeLeft = retVal[ 'Value' ][1]
    chain = X509Chain()
    retVal = chain.loadProxyFromString( pemData )
    if not retVal[ 'OK' ]:
      return retVal
    if requiredLifeTime:
      if timeLeft < requiredLifeTime:
        retVal = self.renewFromMyProxy( userDN, userGroup, lifeTime = requiredLifeTime, chain = chain )
        if not retVal[ 'OK' ]:
          return S_ERROR( "Can't get a proxy for %s seconds: %s" % ( requiredLifeTime, retVal[ 'Message' ] ) )
        chain = retVal[ 'Value' ]
    #Proxy is invalid for some reason, let's delete it
    if not chain.isValidProxy()['Value']:
      self.deleteProxy( userDN, userGroup )
      return S_ERROR( "%s@%s has no proxy registered" % ( userDN, userGroup ) )
    return S_OK( chain )

  def __getVOMSAttribute( self, userGroup, requiredVOMSAttribute = False ):
    csVOMSMappings = CS.getVOMSAttributeForGroup( userGroup )
    if not csVOMSMappings:
      return S_ERROR( "No mapping defined for group %s in the CS" )
    if requiredVOMSAttribute and requiredVOMSAttribute not in csVOMSMappings:
      return S_ERROR( "Required attribute %s is not allowed for group %s" % ( requiredVOMSAttribute, userGroup ) )
    if len( csVOMSMappings ) > 1 and not requiredVOMSAttribute:
      return S_ERROR( "More than one VOMS attribute defined for group %s and none required" % userGroup )
    vomsAttribute = requiredVOMSAttribute
    if not vomsAttribute:
      vomsAttribute = csVOMSMappings[0]
    return S_OK( vomsAttribute )

  def getVOMSProxy( self, userDN, userGroup, requiredLifeTime = False, requestedVOMSAttr = False ):
    """ Get proxy string from the Proxy Repository for use with userDN
        in the userGroup and VOMS attr
    """
    retVal = self.__getVOMSAttribute( userGroup, requestedVOMSAttr )
    if not retVal[ 'OK' ]:
      return retVal
    vomsAttr = retVal[ 'Value' ]

    retVal = self.getProxy( userDN, userGroup, requiredLifeTime )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    print "SECS", chain.getRemainingSecs()

    vomsMgr = VOMS()
    return vomsMgr.setVOMSAttributes( chain , vomsAttr )

  def getRemainingTime( self, userDN, userGroup ):
    """
    Returns the remaining time the proxy is valid
    """
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) FROM `ProxyDB_Proxies`"
    retVal = self._query( "%s WHERE UserDN = '%s' AND UserGroup = '%s'" % ( cmd, userDN, userGroup ) )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    if not data:
      return S_OK( 0 )
    return S_OK( int( data[0][0] ) )

  def getUsers( self, validSecondsLeft = 0 ):
    """ Get all the distinct users from the Proxy Repository. Optionally, only users
        with valid proxies within the given validity period expressed in seconds
    """

    cmd = "SELECT UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    if validSecondsLeft:
      cmd += " WHERE ( UTC_TIMESTAMP() + INTERVAL %d SECOND ) < ExpirationTime" % validSecondsLeft
    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = []
    for record in retVal[ 'Value' ]:
      data.append( { 'DN' : record[0],
                     'group' : record[1],
                     'expirationtime' : record[2],
                     'persistent' : record[3] == 'True' } )
    return S_OK( data )

  def getCredentialsAboutToExpire( self, requiredSecondsLeft, onlyPersistent = True ):
    cmd = "SELECT UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    cmd += " WHERE TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) < %s" % requiredSecondsLeft
    if onlyPersistent:
      cmd += " AND PersistentFlag = 'True'"
    return self._query( cmd )

  def setPersistencyFlag( self, userDN, userGroup, persistent = True ):
    """ Set the proxy PersistentFlag to the flag value
    """
    if persistent:
      sqlFlag="True"
    else:
      sqlFlag="False"
    retVal = self._query( "SELECT PersistentFlag FROM `ProxyDB_Proxies` WHERE UserDN='%s' AND UserGroup='%s'" % ( userDN, userGroup ) )
    sqlInsert = True
    if retVal[ 'OK' ]:
      data = retVal[ 'Value' ]
      if len( data ) > 0:
        sqlInsert = False
        if data[0][0] == sqlFlag:
          return S_OK()
    if sqlInsert:
      #If it's not in the db and we're removing the persistency then do nothing
      if not persistent:
        return S_OK()
      cmd = "INSERT INTO `ProxyDB_Proxies` ( UserDN, UserGroup, Pem, ExpirationTime, PersistentFlag ) VALUES "
      cmd += "( '%s', '%s', '', UTC_TIMESTAMP(), 'True' )" % ( userDN, userGroup )
    else:
      cmd = "UPDATE `ProxyDB_Proxies` SET PersistentFlag='%s' WHERE UserDN='%s' AND UserGroup='%s'" % ( sqlFlag,
                                                                                            userDN,
                                                                                            userGroup )

    retVal = self._update(cmd)
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK()

  def getProxiesContent( self, selDict, sortList, start = 0, limit = 0 ):
    """
    Function to get the contents of the db
      parameters are a filter to the db
    """
    fields = ( "UserDN", "UserGroup", "ExpirationTime", "PersistentFlag" )
    cmd = "SELECT %s FROM `ProxyDB_Proxies` WHERE Pem is not NULL" % ", ".join( fields )
    for field in selDict:
      cmd += " AND (%s)" % " OR ".join( [ "%s=%s" % ( field, self._escapeString( str( value ) )[ 'Value' ] ) for value in selDict[field] ] )
    if sortList:
      cmd += " ORDER BY %s" % ", ".join( [ "%s %s" % ( sort[0], sort[1] ) for sort in sortList ] )
    if limit:
      cmd += " LIMIT %d,%d" % ( start, limit )
    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = []
    for record in retVal[ 'Value' ]:
      record = list( record )
      if record[3] == 'True':
        record[3] = True
      else:
        record[3] = False
      data.append( record )
    totalRecords = len( data )
    cmd = "SELECT COUNT( UserGroup ) FROM `ProxyDB_Proxies`"
    if selDict:
      qr = []
      for field in selDict:
        qr.append( "(%s)" % " OR ".join( [ "%s=%s" % ( field, self._escapeString( str( value ) )[ 'Value' ] ) for value in selDict[field] ] ) )
      cmd += " WHERE %s" % " AND ".join( qr )
    retVal = self._query( cmd )
    if retVal[ 'OK' ]:
      totalRecords = retVal[ 'Value' ][0][0]
    return S_OK( { 'ParameterNames' : fields, 'Records' : data, 'TotalRecords' : totalRecords } )

  def logAction( self, action, issuerDN, issuerGroup, targetDN, targetGroup ):
    """
      Add an action to the log
    """
    cmd = "INSERT INTO `ProxyDB_Log` ( Action, IssuerDN, IssuerGroup, TargetDN, TargetGroup, Timestamp ) VALUES "
    cmd += "( '%s', '%s', '%s', '%s', '%s', UTC_TIMESTAMP() )" % ( action,
                                                                   issuerDN,
                                                                   issuerGroup,
                                                                   targetDN,
                                                                   targetGroup )
    retVal = self._update( cmd )
    if not retVal[ 'OK' ]:
      gLogger.error( "Can't add a log: %s" % retVal[ 'Message' ] )

  def purgeLogs( self ):
    """
    Purge expired requests from the db
    """
    cmd = "DELETE FROM `ProxyDB_Log` WHERE TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > 63936000"
    return self._update( cmd )

  def getLogsContent( self, selDict, sortList, start = 0, limit = 0 ):
    """
    Function to get the contents of the logs table
      parameters are a filter to the db
    """
    fields = ( "Action", "IssuerDN", "IssuerGroup", "TargetDN", "TargetGroup", "Timestamp" )
    cmd = "SELECT %s FROM `ProxyDB_Log`" % ", ".join( fields )
    if selDict:
      qr = []
      if 'beforeDate' in selDict:
        qr.append( "Timestamp < %s" % self._escapeString( selDict[ 'beforeDate' ] )[ 'Value' ] )
        del( selDict[ 'beforeDate' ] )
      if 'afterDate' in selDict:
        qr.append( "Timestamp > %s" % self._escapeString( selDict[ 'afterDate' ] )[ 'Value' ] )
        del( selDict[ 'afterDate' ] )
      for field in selDict:
        qr.append( "(%s)" % " OR ".join( [ "%s=%s" % ( field, self._escapeString( str( value ) )[ 'Value' ] ) for value in selDict[field] ] ) )
      whereStr = " WHERE %s" % " AND ".join( qr )
      cmd += whereStr
    else:
      whereStr = ""
    if sortList:
      cmd += " ORDER BY %s" % ", ".join( [ "%s %s" % ( sort[0], sort[1] ) for sort in sortList ] )
    if limit:
      cmd += " LIMIT %d,%d" % ( start, limit )
    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    totalRecords = len( data )
    cmd = "SELECT COUNT( Timestamp ) FROM `ProxyDB_Log`"
    cmd += whereStr
    retVal = self._query( cmd )
    if retVal[ 'OK' ]:
      totalRecords = retVal[ 'Value' ][0][0]
    return S_OK( { 'ParameterNames' : fields, 'Records' : data, 'TotalRecords' : totalRecords } )