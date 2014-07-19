module Reader
    where

import Network.HTTP (simpleHTTP, getRequest, getResponseBody)
import Protocol
import Data.Maybe (fromJust)
import Control.Monad
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe
import qualified Data.List as DL
import qualified Data.ByteString.Lazy.Char8 as BL
import System.IO (withFile, IOMode(ReadMode), hGetContents)
-- TODO import qualified System.IO.Streams as Streams
-- module for reading file inputs from disco, different schemes

--TODO instead of MaybeT IO -> ErrorT?

data Scheme = SDir | SDisco | SRaw | SHttp deriving(Show, Eq)

--takes whole replica location string, divides it in two part: scheme and actual address
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

-- gets the disco or ddfs part from the address (if it is to be accessed locally)
get_data_type :: String -> Task -> String
get_data_type addr task = maybe "" (fst . break (=='/')) (DL.stripPrefix ((host task) ++ "/")  addr)

-- /usr/local/var/disco/data/localhost ++ sth
--A URL with the disco scheme is to be accessed using HTTP at the disco_port specified in the TASK response from Disco. 
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

-- different locations readers
http_reader :: String -> IO String
http_reader address = do 
    simpleHTTP (getRequest address) >>= getResponseBody

-- hSetEncoding utf8? like in python force_utf8
disco_reader :: String -> Task -> MaybeT IO String
disco_reader addr task = do
    let err_fun = error "Unknown address - disco reader"
    case (get_data_type addr task) of
        "disco" -> do maybe mzero (lift . readFile) (absolute_disco_path addr task)
        "ddfs" -> do maybe mzero ((fmap BL.unpack) . lift . BL.readFile) (absolute_ddfs_path addr task)
        otherwise -> mzero

--obtain dir entries no matter if local or remote
get_dir_lines :: String -> Task -> MaybeT IO String
get_dir_lines addr task = do
    let (new_scheme, conv_addr) = convert_uri SDisco addr task
    case new_scheme of
        SHttp -> lift $  http_reader conv_addr
        otherwise -> disco_reader conv_addr task

dir_reader :: String -> Task -> MaybeT IO [String]
dir_reader addr task = do
    dir_file <- get_dir_lines addr task --TODO catch
    let dir_lines = lines dir_file
    let word_lines = map words dir_lines
    mapM (\[_, file, _] -> address_reader file task) word_lines

-- absolute paths, if data is stored locally
absolute_disco_path :: String -> Task -> Maybe String
absolute_disco_path addr task =
    case (DL.stripPrefix ((host task) ++"/disco") addr) of
        Just rest -> Just $ (disco_data task) ++ rest
        Nothing -> Nothing

absolute_ddfs_path :: String -> Task -> Maybe String
absolute_ddfs_path addr task =
    case (DL.stripPrefix ((host task) ++"/ddfs") addr) of
        Just rest -> Just $ (ddfs_data task) ++ rest
        Nothing -> Nothing

read_inputs :: Task -> [String] -> MaybeT IO [String]
read_inputs task inpt_list = --mapM (\inpt -> address_reader inpt task) inpt_list
    liftM concat $ mapM (\inpt -> read_dir_rest inpt task) inpt_list -- concat because dir returns IO [String]

read_dir_rest :: String -> Task -> MaybeT IO [String]
read_dir_rest address task = do
    (new_scheme, conv_addr) <- maybe mzero (\(scheme, addr) -> return (convert_uri scheme addr task)) (get_scheme address)
    case new_scheme of
        SDir -> do dir_reader conv_addr task 
        _ -> do liftM (:[]) $ address_reader address task

address_reader :: String -> Task -> MaybeT IO String
address_reader address task = do
    (new_scheme, conv_addr) <- maybe mzero (\(scheme, addr) -> return (convert_uri scheme addr task)) (get_scheme address)
    case new_scheme of
        SHttp -> do lift $ http_reader conv_addr
        SDisco -> do disco_reader conv_addr task
        SRaw -> return conv_addr --TODO Base64

disco_output_path :: String -> Task -> String
disco_output_path tempPath task =
    "disco://" ++ (host task) ++ "/disco" ++ path
    where
        Just path = DL.stripPrefix (disco_data task) tempPath --TODO

