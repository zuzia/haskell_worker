module Reader
    where

import Network.HTTP (simpleHTTP, getRequest, getResponseBody)
import Protocol
import Data.Maybe (fromJust)
import qualified Data.List as DL
-- module for reading file inputs from disco, different schemes

{--The replica_location is specified as a URL. The protocol scheme used for the replica_location could be one of http, disco, dir or raw. A URL with the disco scheme is to be accessed using HTTP at the disco_port specified in the TASK response from Disco. The raw scheme denotes that the URL itself (minus the scheme) is the data for the task. The data needs to be properly URL encoded, for instance using Base64 encoding. The dir is like the disco scheme, except that the file pointed to contains lines of the form

<label> ‘SP’ <url> ‘SP’ <output_size> ‘\n’ --}


data Scheme = SDir | SDisco | SRaw | SHttp deriving(Show)

--TODO take hack, but just for now to have it working and not worry about types
http_reader :: String -> IO String
http_reader address = simpleHTTP (getRequest address) >>= fmap (take 100000) . getResponseBody

--TODO disco_reader
--TODO dir_reader
-- form of replica_location: "dir://localhost/disco/localhost/9f/gojob@57a:3a6d2:872f0/.disco/map-0-1402239314715965.results"

--TODO port
get_scheme :: String -> Task -> (Scheme, String)
get_scheme addr task = 
    case scheme of
        "http" -> (SHttp, addr) --TODO
        "disco" -> (SDisco, convert_disco_addr location task)
        "dir" -> (SDir, addr) --TODO
        "raw" -> (SRaw, addr) --TODO
    where
        (scheme, loc) = break (==':') addr
        location = drop 3 loc --get rid of "://"

-- /usr/local/var/disco/data/localhost ++ sth
--TODO change it
convert_disco_addr :: String -> Task -> String
convert_disco_addr addr task =
--    "http://" ++ local_str ++ ":" ++ show(port) ++ rest
    (disco_data task) ++ rest
    where
        rest = fromJust $ DL.stripPrefix "localhost/disco" addr --don't look at me ;)
        --(local_str, rest) = break (=='/') addr
-- TODO abstract task
-- TODO just http implement rest
address_reader :: String -> Task -> IO String
address_reader address task = do
    case scheme of
        SHttp -> do http_reader addr
        SDisco -> do readFile addr --TODO change that
    where
        (scheme, addr) = get_scheme address task

disco_output_path :: String -> Task -> String
disco_output_path tempPath task =
    "disco://" ++ (host task) ++ "/disco" ++ path
    where
        Just path = DL.stripPrefix (disco_data task) tempPath --TODO

