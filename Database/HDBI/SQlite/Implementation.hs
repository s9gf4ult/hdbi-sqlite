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
import Control.Monad (forM, forM_)
import Data.Int
import Data.Typeable
import Database.HDBI.DriverUtils
import Database.HDBI.Formaters
import Database.HDBI.SqlValue
import Database.HDBI.Types
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import qualified Database.SQLite3.Direct as SD

encodeUTF8 :: T.Text -> SD.Utf8
encodeUTF8 x = SD.Utf8 $ toByteString $ fromText x

encodeLUTF8 :: TL.Text -> SD.Utf8
encodeLUTF8 x = SD.Utf8 $ toByteString $ fromLazyText x

encodeSUTF8 :: String -> SD.Utf8
encodeSUTF8 x = SD.Utf8 $ toByteString $ fromString x

decodeUTF8 :: SD.Utf8 -> T.Text
decodeUTF8 (SD.Utf8 x) = T.decodeUtf8 x

decodeLUTF8 :: SD.Utf8 -> TL.Text
decodeLUTF8 (SD.Utf8 x) = TL.decodeUtf8 $ BL.fromChunks [x]

data SQliteConnection = SQliteConnection
                        { scDatabase :: MVar (Maybe SD.Database)
                        , scConnString :: T.Text
                        , scStatements :: ChildList SQliteStatement
                        }
                        deriving (Typeable)

data SQliteStatement = SQliteStatement
                       { ssState :: MVar SQState
                       , ssConnection :: SQliteConnection
                       , ssQuery :: Query
                       }
                       deriving (Typeable)

data SQState = SQNew { sqStatement :: SD.Statement }
             | SQExecuted { sqStatement :: SD.Statement
                          , sqResult :: SD.StepResult
                          }
             | SQFetching { sqStatement :: SD.Statement
                          , sqResult :: SD.StepResult
                          }
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
               <*> newChildList

instance Connection SQliteConnection where
  type ConnStatement SQliteConnection = SQliteStatement

  disconnect conn = modifyMVar_ (scDatabase conn) $ \c -> case c of
    Nothing -> return Nothing
    Just (con) -> do
      closeAllChildren (scStatements conn)
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
      Left err -> throwErrMsg con $ show err
      Right x -> case x of
        Nothing -> throwErrMsg con ""
        Just st -> do
          ret <- SQliteStatement
                 <$> (newMVar $ SQNew st)
                 <*> return conn
                 <*> return query
          addChild (scStatements conn) ret
          return ret

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
    SQExecuted {} -> throwIO $ SqlDriverError $ sqliteMsg "Statement is already executed"
    SQFetching {} -> throwIO $ SqlDriverError $ sqliteMsg "Statement is already executed (fetching)"
    SQFinished -> throwIO $ SqlDriverError $ sqliteMsg "Statement is already finished"

    where
      execute' st = do
        withConnectionUnlocked (ssConnection stmt)
          $ \con -> forM_ (zip [1..] vals) $ uncurry $ bindParam con st
        res <- SD.step st      -- if this is INSERT or UPDATE query we need
                              -- step to execute it.
        case res of
          Left err -> withConnectionUnlocked (ssConnection stmt)
                      $ \con -> throwErrMsg con $ show err
          Right eres  -> return $ SQExecuted st eres

  -- executeRaw: use default
  -- executeMany: use default implementation
  statementStatus stmt = do
    res <- readMVar $ ssState stmt
    return $ case res of
      SQNew {}             -> StatementNew
      SQExecuted {}        -> StatementExecuted
      SQFetching _ SD.Row  -> StatementExecuted
      SQFetching _ SD.Done -> StatementFetched
      SQFinished           -> StatementFinished

  -- affectedRows: can not be implemented safely because database has no this
  -- feature

  finish stmt = modifyMVar_ (ssState stmt) $ \st -> case st of
    SQFinished -> return SQFinished
    x -> do
      res <- SD.finalize $ sqStatement x
      case res of
        Left err -> withConnectionUnlocked (ssConnection stmt)
                    $ \con -> throwErrMsg con $ show err
        Right ()  -> return SQFinished

  reset stmt = modifyMVar_ (ssState stmt) $ \st -> case st of
    SQFinished -> withConnectionUnlocked (ssConnection stmt) $ \con -> do
      res <- SD.prepare con
             $ encodeLUTF8
             $ unQuery $ ssQuery stmt
      case res of
        Left err -> throwErrMsg con $ show err
        Right mst -> case mst of
          Nothing -> throwErrMsg con ""
          Just s -> return $ SQNew s
    x -> do
      let rst = sqStatement x
      res <- SD.reset rst
      case res of
        Left err -> withConnectionUnlocked (ssConnection stmt)
                    $ \con -> throwErrMsg con $ show err
        Right () -> do
          SD.clearBindings rst
          return $ SQNew rst

  fetchRow stmt = modifyMVar (ssState stmt) $ \st -> case st of
    SQNew _ -> throwIO $ SqlDriverError
               $ sqliteMsg "Statement is not executed to fetch rows from"
    SQExecuted x SD.Row      -> fetch' x
    SQExecuted x SD.Done     -> return (SQFetching x SD.Done, Nothing)
    SQFetching x SD.Row      -> fetch' x
    r@(SQFetching _ SD.Done) -> return (r, Nothing)
    SQFinished -> throwIO $ SqlDriverError
                  $ sqliteMsg "Statement is already finished to fetch rows from"
    where
      fetch' :: SD.Statement -> IO (SQState, Maybe [SqlValue])
      fetch' ss = do
        cc <- SD.columnCount ss
        res <- forM [0..cc-1] $ \col -> do
          ct <- SD.columnType ss col
          case ct of
            SD.IntegerColumn -> SqlInteger . toInteger
                                <$> SD.columnInt64 ss col
            SD.FloatColumn -> SqlDouble <$> SD.columnDouble ss col
            SD.TextColumn -> SqlText . decodeLUTF8
                             <$> SD.columnText ss col
            SD.BlobColumn -> SqlBlob <$> SD.columnBlob ss col
            SD.NullColumn -> return SqlNull
        st <- SD.step ss
        case st of
          Left err -> withConnectionUnlocked (ssConnection stmt)
                      $ \con -> throwErrMsg con $ show err
          Right sres -> return (SQFetching ss sres, Just res)

  getColumnNames stmt = do
    st <- readMVar $ ssState stmt
    case st of
      SQNew {} -> throwIO $ SqlDriverError $ sqliteMsg "Statement is not executed to get column names"
      SQFinished -> throwIO $ SqlDriverError $ sqliteMsg "Statement is already finished to get column names"
      x -> do
        let sqst = sqStatement x
        cols <- SD.columnCount sqst
        forM [0..cols-1] $ \col -> do
          res <- SD.columnName sqst col
          case res of
            Nothing -> return ""
            Just val -> return $ decodeLUTF8 val

  getColumnsCount stmt = do
    st <- readMVar $ ssState stmt
    case st of
      SQNew {} -> throwIO $ SqlDriverError $ sqliteMsg "Statement is not executed to get columns count"
      SQFinished -> throwIO $ SqlDriverError $ sqliteMsg "Statement is already finished to get columns count"
      x -> do
        (SD.ColumnIndex idx) <- SD.columnCount $ sqStatement x
        return idx

  originalQuery stmt = ssQuery stmt

bindParam :: SD.Database -> SD.Statement -> SD.ParamIndex -> SqlValue -> IO ()
bindParam con st idx val = do
  res <- bind val
  case res of
    Left err -> throwErrMsg con $ show err
    Right ()  -> return ()
  where
    binds x = SD.bindText st idx $ encodeSUTF8 x

    bindShow :: (Show a) => a -> IO (Either SD.Error ())
    bindShow x =  binds $ show x

    bindi i = SD.bindInt64 st idx i

    downInt64 :: Integer -> Maybe Int64
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
    bind (SqlText t) = SD.bindText st idx $ encodeLUTF8 t
    bind (SqlBlob b) = SD.bindBlob st idx b
    bind (SqlBool b) = bindi $ if b then 1 else 0
    bind (SqlBitField bf) = bindShow bf
    bind (SqlUUID u) = bindShow u
    bind (SqlUTCTime ut) = binds $ formatIsoUTCTime ut
    bind (SqlLocalDate d) = binds $ formatIsoDay d
    bind (SqlLocalTimeOfDay td) = binds $ formatIsoTimeOfDay td
    bind (SqlLocalTime lt) = binds $ formatIsoLocalTime lt
    bind SqlNull = SD.bindNull st idx


throwErrMsg :: SD.Database -> String -> IO a
throwErrMsg db err = do
  (SD.Utf8 errmsg) <- SD.errmsg db
  throwIO $ SqlError err $ T.unpack $ T.decodeUtf8 errmsg

sqliteMsg :: String -> String
sqliteMsg = ("hdbi-sqlite: " ++)

withConnectionUnlocked :: SQliteConnection -> (SD.Database -> IO a) -> IO a
withConnectionUnlocked conn fun = do
  val <- readMVar $ scDatabase $ conn
  case val of
    Nothing -> throwIO $ SqlDriverError
               $ sqliteMsg $ "connection is closed"
    Just x  -> fun x
