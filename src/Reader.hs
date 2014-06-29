module Reader
    where

import Network.HTTP (simpleHTTP, getRequest, getResponseBody)
import Protocol
import Data.Maybe (fromJust)
import Control.Monad
import qualified Data.List as DL
import qualified Data.ByteString.Lazy.Char8 as BL
import System.IO (withFile, IOMode(ReadMode), hGetContents)
-- TODO import qualified System.IO.Streams as Streams
-- module for reading file inputs from disco, different schemes

{--The replica_location is specified as a URL. The protocol scheme used for the replica_location could be one of http, disco, dir or raw. A URL with the disco scheme is to be accessed using HTTP at the disco_port specified in the TASK response from Disco. The raw scheme denotes that the URL itself (minus the scheme) is the data for the task. The data needs to be properly URL encoded, for instance using Base64 encoding. The dir is like the disco scheme, except that the file pointed to contains lines of the form

<label> ‘SP’ <url> ‘SP’ <output_size> ‘\n’ --}


data Scheme = SDir | SDisco | SRaw | SHttp deriving(Show)

--takes whole replica location string, divides it in two part: scheme and actual address
split_scheme_loc :: String -> (String, String)
split_scheme_loc addr = (\(scheme, loc) -> (scheme, drop 3 loc)) $ break (==':') addr

get_scheme :: String -> Task -> (Scheme, String)
get_scheme addr task = 
    case scheme of
        "http" -> (SHttp, addr)
        "https" -> (SHttp, addr)
        "disco" -> (SDisco, location)
        "dir" -> (SDir, location)
        "raw" -> (SRaw, location)
    where
        (scheme, location) = split_scheme_loc addr

-- gets the disco or ddfs part from the address (if it is to be accessed locally)
get_data_type :: String -> Task -> String
get_data_type addr task = fst $ break (=='/') $ fromJust $ DL.stripPrefix ((host task) ++ "/")  addr

-- /usr/local/var/disco/data/localhost ++ sth
--A URL with the disco scheme is to be accessed using HTTP at the disco_port specified in the TASK response from Disco. 
--TODO dir
convert_uri :: Scheme -> String -> Task -> (Scheme, String)
convert_uri SDisco addr task =
    case ((host task) /= local_str) of
        True -> (SHttp, "http://" ++ local_str ++ ":" ++ show(disco_port task) ++ rest)
        False -> (SDisco, addr)
    where
        (local_str, rest) = break (=='/') addr
convert_uri schem addr _ = (schem, addr)

-- different locations readers
http_reader :: String -> IO String
http_reader address = do 
    simpleHTTP (getRequest address) >>= getResponseBody

-- hSetEncoding utf8? like in python force_utf8
disco_reader :: String -> Task -> IO String
disco_reader addr task = do
    case (get_data_type addr task) of
        "disco" -> do readFile $ absolute_disco_path addr task
        "ddfs" -> do fmap BL.unpack $ BL.readFile $ absolute_ddfs_path addr task --had problems with encoding

dir_reader :: String -> Task -> IO [String]
dir_reader addr task = do
    let path = (disco_data task) ++ "/" ++ addr --TODO? path = absolute_disco_path addr task
    dir_lines <- fmap lines $ withFile path ReadMode hGetContents
    let word_lines = map words dir_lines
    mapM (\[_, file, _] -> address_reader file task) word_lines

-- form of replica_location: "dir://localhost/disco/localhost/9f/gojob@57a:3a6d2:872f0/.disco/map-0-1402239314715965.results"

-- absolute paths, if data is stored locally
absolute_disco_path :: String -> Task -> String
absolute_disco_path addr task =
    (disco_data task) ++ rest
    where
        rest = fromJust $ DL.stripPrefix ((host task) ++"/disco") addr

--ddfs data "disco://localhost/ddfs/vol0/blob/1d/bigfile_txt-0$57b-18eb6-f183a\"]]]]]"
absolute_ddfs_path :: String -> Task -> String
absolute_ddfs_path addr task =
    (ddfs_data task) ++ rest
    where
        rest = fromJust $ DL.stripPrefix ((host task) ++"/ddfs") addr

absolute_dir_path :: String -> Task -> String
absolute_dir_path addr task =
    (disco_data task) ++ addr

read_inputs :: Task -> [String] -> IO [String]
read_inputs task inpt_list = --mapM (\inpt -> address_reader inpt task) inpt_list
    liftM concat $ mapM (\inpt -> read_dir_rest inpt task) inpt_list -- concat because dir returns IO [String]

read_dir_rest :: String -> Task -> IO [String]
read_dir_rest address task =
    let (scheme, addr) = get_scheme address task
        (new_scheme, conv_addr) = convert_uri scheme addr task in
    case new_scheme of
        SDir -> do dir_reader conv_addr task 
        _ -> do liftM (:[]) $ address_reader address task

-- TODO abstract task
address_reader :: String -> Task -> IO String
address_reader address task = do
    let (scheme, addr) = get_scheme address task --addr is "http://..." or path to file (not absolute)
    let (new_scheme, conv_addr) = convert_uri scheme addr task
    case new_scheme of
        SHttp -> do http_reader conv_addr
        SDisco -> do disco_reader conv_addr task
        SRaw -> return conv_addr --TODO Base64

disco_output_path :: String -> Task -> String
disco_output_path tempPath task =
    "disco://" ++ (host task) ++ "/disco" ++ path
    where
        Just path = DL.stripPrefix (disco_data task) tempPath --TODO

