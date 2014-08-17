{-|
Module      : Reader
Description : Reading file inputs from different locations

Reading inputs using schemes: dir, disco, raw, http.
Converting addresses to local/http when necessary.
-}
module Reader(
    read_inputs, -- :: Task -> [String] -> IO [String]
    disco_output_path, -- :: String -> Task -> String
    split_scheme_loc, -- :: String -> (String, String)
    http_reader -- :: String -> IO String
) where

import Protocol
import Errors
import Network.HTTP (simpleHTTP, getRequest, getResponseBody) --TODO use Network.HTTP.Conduit instead
import Data.Maybe (fromJust)
import Control.Monad
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe
import qualified Data.List as DL
import qualified Data.ByteString.Lazy.Char8 as BL
import System.IO (withFile, IOMode(ReadMode), hGetContents)
import Control.Exception
import Prelude hiding (catch)

data Scheme = SDir | SDisco | SRaw | SHttp deriving(Show, Eq)

-- | Takes whole replica location string, divides it in two part: scheme and remaining address
split_scheme_loc :: String -> (String, String)
split_scheme_loc addr = (\(scheme, loc) -> (scheme, drop 3 loc)) $ break (==':') addr

get_scheme :: String -> Maybe (Scheme, String)
get_scheme addr = 
    case scheme of
        "http" -> Just (SHttp, addr)
        "https" -> Just (SHttp, addr)
        "disco" -> Just (SDisco, location)
        "dir" -> Just (SDir, location)
        "raw" -> Just (SRaw, location)
        otherwise -> Nothing
    where
        (scheme, location) = split_scheme_loc addr

-- | Gets the disco or ddfs part from the address
get_data_type :: String -> Task -> String
get_data_type addr task = maybe "" (fst . break (=='/')) (DL.stripPrefix ((host task) ++ "/")  addr)

-- | Converts disco/ddfs address to http address with added port (if it is not local)
convert_uri :: Scheme -> String -> Task -> (Scheme, String)
convert_uri scheme addr task =
    case scheme of
        SDisco -> conv_helper scheme addr task
        otherwise -> (scheme, addr)

conv_helper :: Scheme -> String -> Task -> (Scheme, String)
conv_helper scheme addr task =
    case ((host task) /= local_str) of
        True -> (SHttp, "http://" ++ local_str ++ ":" ++ show(disco_port task) ++ rest)
        False -> (scheme, addr)
    where
        (local_str, rest) = break (=='/') addr

-- | Access input via http
http_reader :: String -> IO String
http_reader address = do 
    resp <- try (simpleHTTP (getRequest address) >>= getResponseBody) :: IO (Either IOException String)
    case resp of
        Right r -> return r
        Left e -> throwIO $ HttpExcept $ show e

-- | Access input locally (disco and ddfs)
disco_reader :: String -> Task -> IO String
disco_reader addr task = do
    case (get_data_type addr task) of
        "disco" -> do 
            res <- try (readFile (absolute_disco_path addr task)) :: IO (Either IOException String)
            case res of
                Right r -> return r
                Left e -> throwIO $ DiscoExcept $ show e
        "ddfs" -> do
            res <- try $ ((fmap BL.unpack) . BL.readFile) (absolute_ddfs_path addr task) :: IO (Either IOException String)
            case res of
                Right r -> return r
                Left e -> throwIO $ DdfsExcept $ show e
        otherwise -> throwIO BadAddress

-- | Obtain dir entries no matter if local or remote
get_dir_lines :: String -> Task -> IO String
get_dir_lines addr task = do
    let (new_scheme, conv_addr) = convert_uri SDisco addr task
    case new_scheme of
        SHttp -> http_reader conv_addr
        otherwise -> disco_reader conv_addr task

-- | Read dir file, obtain all the entries and read them
dir_reader :: String -> Task -> IO [String]
dir_reader addr task = do
    res <- try (get_dir_lines addr task) :: IO (Either IOException String)
    case res of
        Right dir_file -> do
            let dir_lines = lines dir_file
            let word_lines = map words dir_lines
            mapM (\[_, file, _] -> address_reader file task) word_lines --TODO try
        Left e -> throwIO e --TODO handle that

-- | Get absolute paths, if data is stored locally
absolute_disco_path :: String -> Task -> String
absolute_disco_path addr task =
    case (DL.stripPrefix ((host task) ++"/disco") addr) of
        Just rest -> (disco_data task) ++ rest
        Nothing -> throw AbsPathExcept

absolute_ddfs_path :: String -> Task -> String
absolute_ddfs_path addr task =
    case (DL.stripPrefix ((host task) ++"/ddfs") addr) of
        Just rest -> (ddfs_data task) ++ rest
        Nothing -> throw AbsPathExcept

-- | Used in Worker module to read the inputs
read_inputs :: Task -> [String] -> IO [String]
read_inputs task inpt_list =
    liftM concat $ mapM (\inpt -> read_dir_rest inpt task) inpt_list -- concat because dir returns IO [String]

-- | First we have to check whether there is a dir scheme input, then read it
-- Dir scheme is handled differently because of multiple entries per one address.
-- Otherwise we just proceed to normal http/disco reading.
read_dir_rest :: String -> Task -> IO [String]
read_dir_rest address task = do
    let schem_addr = fmap (\(scheme, addr) -> convert_uri scheme addr task) (get_scheme address)
    case schem_addr of
        Just (SDir, conv_addr) -> do 
            res <- try (dir_reader conv_addr task) :: IO (Either SomeException [String])
            case res of
                Right r -> return r
                Left e -> throwIO e
        Just _ -> do 
            res <- try $ address_reader address task :: IO (Either SomeException String)
            case res of
                Right r -> return (r:[])
                Left e -> throwIO e
        Nothing -> throwIO UnknownScheme

-- | Identify the scheme and run appropriate function
address_reader :: String -> Task -> IO String
address_reader address task = do
    let schem_addr = fmap (\(scheme, addr) -> convert_uri scheme addr task) (get_scheme address)
    case schem_addr of
        Just (SHttp, conv_addr) -> do http_reader conv_addr
        Just (SDisco, conv_addr) -> do disco_reader conv_addr task
        Just (SRaw, conv_addr) -> return conv_addr
        Nothing -> throwIO UnknownScheme

-- | Return output path that is later send to disco
disco_output_path :: String -> Task -> String
disco_output_path tempPath task =
    "disco://" ++ (host task) ++ "/disco" ++ path
    where
        Just path = DL.stripPrefix (disco_data task) tempPath

