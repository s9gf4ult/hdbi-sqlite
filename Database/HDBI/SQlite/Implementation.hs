{-# LANGUAGE
  TypeFamilies
, DeriveDataTypeable
  #-}

module Database.HDBI.SQlite.Implementation where

import Data.Typeable
import Blaze.ByteString.Builder (toByteString)
import Blaze.ByteString.Builder.Char.Utf8 (fromText, fromLazyText)
import Control.Applicative
import Control.Exception
import Control.Concurrent.MVar
import Database.HDBI.SqlValue
import Database.HDBI.Types
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Database.SQLite3.Direct as SD


data SQliteConnection = SQliteConnection
                        { scDatabase :: MVar (Maybe SD.Database) }
                        deriving (Typeable)

data SQliteStatement = SQliteStatement
                       { ssStatement :: SD.Statement
                       , ssQuery :: Query
                       }
                       deriving (Typeable)

connectSqlite3 :: T.Text -> IO SQliteConnection
connectSqlite3 connstr = do
  res <- SD.open $ SD.Utf8 $ toByteString $ fromText connstr
  case res of
    Left (err, (SD.Utf8 errmsg)) -> throwIO $ SqlError (show err)
                                    $ T.unpack $ T.decodeUtf8 $ errmsg
    Right r -> SQliteConnection
               <$> newMVar (Just r)

instance Connection SQliteConnection where
  type ConnStatement SQliteConnection = SQliteStatement
  
  disconnect conn = modifyMVar_ (scDatabase conn) $ \con -> case con of
    Nothing -> return Nothing
    Just (con) -> do
      res <- SD.close con
      case res of
        Left err  -> throwIO $ SqlError (show err) "Could not close the database"
        Right () -> return Nothing

  begin conn = undefined

  commit conn = undefined

  rollback conn = undefined

  inTransaction conn = undefined

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
        Just st -> return $ SQliteStatement st query

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

instance Statement SQliteStatement where
