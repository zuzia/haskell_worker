{-# LANGUAGE DeriveDataTypeable #-}

module Errors
    where
import Control.Exception
import Control.Monad.Trans.Maybe
import Data.Typeable
import Protocol

data WorkerException = 
    UnknownScheme 
    | BadAddress 
    | DiscoExcept String
    | DdfsExcept String
    | HttpExcept String
    | AbsPathExcept
    | SortingExcept
    | MiscExcept String deriving (Typeable, Eq)

instance Show WorkerException where
    show UnknownScheme = "Unknown scheme"
    show BadAddress = "Bad address - disco/ddfs reader" 
    show (HttpExcept str) = "Couldn't read data via http, message: " ++ str
    show (DiscoExcept str) = "Couldn't read disco data, message: " ++ str
    show (DdfsExcept str) = "Couldn't read ddfs data, message: " ++ str
    show AbsPathExcept = "Absolute path exception disco/ddfs reader" 
    show SortingExcept = "Problem with sorting reduce input" 
    show (MiscExcept str) = str

instance Exception WorkerException

--TODO add logging
exception_handler :: WorkerException -> IO ()
exception_handler err = do
    runMaybeT $ exchange_msg $ W_msg (show err)
    return ()
