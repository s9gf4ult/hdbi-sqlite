{-# LANGUAGE
  TypeFamilies
, DeriveDataTypeable
, OverloadedStrings
  #-}

module Database.HDBI.SQlite.Implementation where

import Blaze.ByteString.Builder (toByteString)
import Blaze.ByteString.Builder.Char.Utf8 (fromText, fromLazyText, fromString)
import Control.Applicative
import Control.Concurrent.MVar
import Control.Exception
import Data.Typeable
import Database.HDBI.Formaters
import Database.HDBI.SqlValue
import Database.HDBI.Types
import qualified Data.ByteString as B
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Database.SQLite3.Direct as SD

encodeUTF8 :: T.Text -> SD.Utf8
encodeUTF8 x = SD.Utf8 $ toByteString $ fromText x

encodeLUTF8 :: TL.Text -> SD.Utf8
encodeLUTF8 x = SD.Utf8 $ toByteString $ fromLazyText x

encodeSUTF8 :: String -> SD.Utf8
encodeSUTF8 x = SD.Utf8 $ toByteString $ fromString x

decodeUTF8 :: SD.Utf8 -> T.Text
decodeUTF8 (SD.Utf8 x) = T.decodeUtf8 x

data SQliteConnection = SQliteConnection
                        { scDatabase :: MVar (Maybe SD.Database)
                        , scConnString :: T.Text
                        }
                        deriving (Typeable)

data SQliteStatement = SQliteStatement
                       { ssState :: MVar SQState
                       , ssConnection :: SQliteConnection
                       , ssQuery :: Query
                       }
                       deriving (Typeable)

data SQState = SQNew { sqStatement :: SD.Statement }
             | SQBinded { sqStatement :: SD.Statement }
             | SQEmpty  { sqStatement :: SD.Statement }
             | SQFinished

connectSqlite3 :: T.Text -> IO SQliteConnection
connectSqlite3 connstr = do
  res <- SD.open $ SD.Utf8 $ toByteString $ fromText connstr
  case res of
    Left (err, (SD.Utf8 errmsg)) -> throwIO $ SqlError (show err)
                                    $ T.unpack $ T.decodeUtf8 $ errmsg
    Right r -> SQliteConnection
               <$> newMVar (Just r)
               <*> return connstr

instance Connection SQliteConnection where
  type ConnStatement SQliteConnection = SQliteStatement

  disconnect conn = modifyMVar_ (scDatabase conn) $ \con -> case con of
    Nothing -> return Nothing
    Just (con) -> do
      res <- SD.close con
      case res of
        Left err  -> throwIO $ SqlError (show err) "Could not close the database"
        Right () -> return Nothing

  begin conn = runRaw conn "begin"

  commit conn = runRaw conn "commit"

  rollback conn = runRaw conn "rollback"

  inTransaction conn = withConnectionUnlocked conn $ \con -> do
    ac <- SD.getAutoCommit con
    return $ not ac               -- we are not in autocommit, so we inside the
                                -- transaction

  connStatus conn = do
    val <- readMVar $ scDatabase $ conn
    return $ case val of
      Nothing -> ConnDisconnected
      Just _  -> ConnOK

  prepare conn query = withConnectionUnlocked conn $ \con -> do
    res <- SD.prepare con $ SD.Utf8 $ toByteString $ fromLazyText $ unQuery query
    case res of
      Left err -> throwErrMsg con err
      Right x -> case x of
        Nothing -> throwIO $ SqlError "" "expression contains no Sql statements"
        Just st -> SQliteStatement
                   <$> (newMVar $ SQNew st)
                   <*> conn
                   <*> query

  -- run: using default implementation

  runRaw conn (Query t) = withConnectionUnlocked conn $ \con -> do
    res <- SD.exec con $ encodeLUTF8 t
    case res of
      Left (err, errmsg) -> throwIO $ SqlError (show err) $ T.unpack $ decodeUTF8 errmsg
      Right () -> return ()

  -- runMany: using default implementation

  clone conn = connectSqlite3 $ scConnString conn
  hdbiDriverName = const "sqlite3"
  dbTransactionSupport = const True


instance Statement SQliteStatement where
  execute stmt vals = modifyMVar_ (ssState stmt) $ \state -> case state of
    SQNew st -> execute' st
    r@(SQBinded {}) -> return r  --  FIXME: maybe throw error here?
    r@(SQEmpty {})  -> throwIO $ SqlDriverError "Statement is already executed"
    SQFinished -> throwIO $ SqlDriverError "Statement is already finished"
      where
        execute' st = do
          forM_ (zip [1..] vals) $ curry $ bindParam st
          res <- SD.step st      -- if this is INSERT or UPDATE query we need
                                -- step to execute it.
          case res of
            Left err -> withConnectionUnlocked (ssConnection stmt)
                        $ \con -> throwErrMsg con err
            Right eres  -> case eres of
              SD.Row -> return $ SQBinded st
              SD.Done -> return $ SQEmpty st

  -- executeRaw: use default
  -- executeMany: use default implementation
  statementStatus stmt = do
    res <- readMVar $ ssState stmt
    return $ case res of
      SQNew {} -> StatementNew
      SQBinded {} -> StatementExecuted
      SQEmpty {} -> StatementFetched
      SQFinished -> StatementFinished

  -- affectedRows: can not be implemented safely because database has no this
  -- feature

  finish stmt = modifyMVar_ (ssState stmt) $ \st -> case st of
    SQFinished -> return SQFinished
    x -> do
      res <- SD.finalize $ sqStatement x
      case res of
        Left err -> withConnectionUnlocked (ssConnection stmt)
                    $ \con -> throwErrMsg con err
        Right ()  -> return SQFinished

  -- reset stmt = modifyMVar_ (ssState stmt) $ \st -> case st of


bindParam :: SD.Statement -> SD.ParamIndex -> SqlValue -> IO ()
bindParam st idx val = do
  res <- bind bal
  case res of
    Left err -> withConnectionUnlocked (ssConnection st)
                $ \con -> throwErrMsg con err
    Right () -> return ()
  where
    binds x = SD.bindText st idx $ encodeSUTF8 x
    bindShow x =  binds $ show x
    bindi i = SD.bindInt64 st idx i

    downInt64 i = if i > imax || i < imin
                  then Nothing
                  else Just $ fromInteger i
      where
        imax = toInteger (maxBound :: Int64)
        imin = toInteger (minBound :: Int64)

    bind (SqlDecimal d) = bindShow d
    bind (SqlInteger i) = case downInt64 i of
      Nothing  -> bindShow i
      Just i64 -> bindi i64
    bind (SqlDouble d) = SD.bindDouble st idx d
    bind (SqlText t) = bindText st idx $ encodeLUTF8 t
    bind (SqlBlob b) = bindBlob st idx b
    bind (SqlBitField bf) = bindShow bf
    bind (SqlUUID u) = bindShow u
    bind (SqlUTCTime ut) = binds $ formatIsoUTCTime ut
    bind (SqlLocalDate d) = binds $ formatIsoDay d
    bind (SqlLocalTimeOfDay td) = binds $ formatIsoTimeOfDay td
    bind (SqlLocalTime lt) = binds $ formatIsoLocalTime lt
    bind SqlNull = SD.bindNull st idx


throwErrMsg :: SD.Database -> SD.Error -> IO a
throwErrMsg db err = do
  (SD.Utf8 errmsg) <- SD.errmsg db
  throwIO $ SqlError (show err) $ T.unpack $ T.decodeUtf8 errmsg

sqliteMsg :: String -> String
sqliteMsg = ("hdbi-sqlite: " ++)

withConnectionUnlocked :: SQliteConnection -> (SD.Database -> IO a) -> IO a
withConnectionUnlocked conn fun = do
  val <- readMVar $ scDatabase $ conn
  case val of
    Nothing -> throwIO $ SqlDriverError
               $ sqliteMsg $ "connection is closed"
    Just x  -> fun x

