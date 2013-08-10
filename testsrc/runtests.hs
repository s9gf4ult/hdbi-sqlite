{-# LANGUAGE
  OverloadedStrings
, ScopedTypeVariables
, FlexibleInstances
, FlexibleContexts
  #-}

module Runtests where

import Control.Exception (bracket)
import Database.HDBI
import Database.HDBI.SQlite.Implementation
import Database.HDBI.Tests (allTests, TestFieldTypes(..))
import Test.Framework

fields :: TestFieldTypes
fields = TestFieldTypes
         { tfDecimal = ""
         , tfInteger = ""
         , tfDouble = ""
         , tfText = ""
         , tfBlob = ""
         , tfBool = ""
         , tfBitField = ""
         , tfUUID = ""
         , tfUTCTime = ""
         , tfLocalDate = ""
         , tfLocalTimeOfDay = ""
         , tfLocalTime = ""
         }


main :: IO ()
main = bracket
       (connectSqlite3 ":memory:")
       disconnect
       $ \c -> defaultMain [ allTests fields c ]
