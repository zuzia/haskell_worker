module Reader
    where

import Network.HTTP (simpleHTTP, getRequest, getResponseBody)
import Protocol
import Data.Maybe (fromJust)
import qualified Data.List as DL
-- TODO import qualified System.IO.Streams as Streams
-- module for reading file inputs from disco, different schemes

{--The replica_location is specified as a URL. The protocol scheme used for the replica_location could be one of http, disco, dir or raw. A URL with the disco scheme is to be accessed using HTTP at the disco_port specified in the TASK response from Disco. The raw scheme denotes that the URL itself (minus the scheme) is the data for the task. The data needs to be properly URL encoded, for instance using Base64 encoding. The dir is like the disco scheme, except that the file pointed to contains lines of the form

<label> ‘SP’ <url> ‘SP’ <output_size> ‘\n’ --}


data Scheme = SDir | SDisco | SRaw | SHttp deriving(Show)

http_reader :: String -> IO String
http_reader address = simpleHTTP (getRequest address) >>= getResponseBody

-- how nice, string manipulation in haskell
get_data_type :: String -> Task -> String
get_data_type addr task = fst $ break (=='/') $ fromJust $ DL.stripPrefix ((host task) ++ "/")  addr

disco_reader :: String -> Task -> IO String
disco_reader addr task = do
    let abs_path = case (get_data_type addr task) of
                    "disco" -> absolute_disco_path addr task
                    "ddfs" -> absolute_ddfs_path addr task
    readFile addr

--TODO dir_reader
-- form of replica_location: "dir://localhost/disco/localhost/9f/gojob@57a:3a6d2:872f0/.disco/map-0-1402239314715965.results"

get_scheme :: String -> Task -> (Scheme, String)
get_scheme addr task = 
    case scheme of
        "http" -> (SHttp, addr) --TODO
        "https" -> (SHttp, addr)
--        "disco" -> (SDisco, convert_disco_addr location task)
        "disco" -> (SDisco, absolute_disco_path location task)
        "dir" -> (SDir, absolute_dir_path location task) --TODO
        "raw" -> (SRaw, location) --TODO
    where
        (scheme, loc) = break (==':') addr
        location = drop 3 loc --get rid of "://"

absolute_disco_path :: String -> Task -> String
absolute_disco_path addr task =
    (disco_data task) ++ rest
    where
        rest = fromJust $ DL.stripPrefix ((host task) ++"/disco") addr

absolute_ddfs_path :: String -> Task -> String
absolute_ddfs_path addr task =
    (ddfs_data task) ++ rest
    where
        rest = fromJust $ DL.stripPrefix ((host task) ++"/ddfs") addr

absolute_dir_path :: String -> Task -> String
absolute_dir_path addr task =
    (disco_data task) ++ addr

split_scheme_loc :: String -> (String, String)
split_scheme_loc addr = (\(scheme, loc) -> (scheme, drop 3 loc)) $ break (==':') addr
-- /usr/local/var/disco/data/localhost ++ sth
--A URL with the disco scheme is to be accessed using HTTP at the disco_port specified in the TASK response from Disco. 
--TODO dir
convert_uri :: Scheme -> String -> Task -> String
convert_uri SDisco addr task =
    case ((host task) /= local_str) of
        True -> "http://" ++ local_str ++ ":" ++ show(disco_port task) ++ rest
        False -> addr
    where
        (local_str, rest) = break (=='/') addr
convert_uri _ addr _ = addr

-- TODO abstract task
-- TODO just http implement rest
-- TODO iterate over the list of inputs
address_reader :: String -> Task -> IO String
address_reader address task = do
    case scheme of
        SHttp -> do http_reader addr
--        SDisco -> do readFile addr --TODO change that
--        SDisco -> do disco_reader conv_addr task
        SDisco -> do disco_reader addr task
--        SDir -> do dir_reader conv_addr
        SRaw -> return addr
    where
        (scheme, addr) = get_scheme address task
--        conv_addr = convert_uri scheme addr task

disco_output_path :: String -> Task -> String
disco_output_path tempPath task =
    "disco://" ++ (host task) ++ "/disco" ++ path
    where
        Just path = DL.stripPrefix (disco_data task) tempPath --TODO

