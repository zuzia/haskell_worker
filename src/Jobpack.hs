{-# LANGUAGE OverloadedStrings #-}
{-|
Module      : Jobpack
Description : Jobpack creation and submission

Needs debugging and improvements.
-}

module Jobpack
    where

--import Pipeline
import Jobutil
import Data.List as DL
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Aeson
import Data.Binary as B --binary serialization of haskell data, big endian order
import Data.Bits (shiftL)
import System.IO
import System.Posix.Files
import System.Posix.User (getEffectiveUserName)
import System.Directory
import qualified Data.Map as Map
--import Codec.Compression.Zlib.Raw --RFC1951 Deflated compression format
import Control.Exception
import qualified Network.HTTP as Http
import qualified Network.TCP as TCP
import Network.URI
import Network.BSD (getHostName)

--TODO get rid of this it's in Pipeline
data Grouping
    = Split
    | Group_label
    | Group_node
    | Group_node_label
    | Group_all

get_magic :: Word16
get_magic = 0xd5c0
get_version1 :: Word16
get_version1 = 0x0001
get_version2 :: Word16
get_version2 = 0x0002
get_header_size :: Int
get_header_size = 128

data Jobpack = Jobpack {
    jobdict :: Jobdict, 
    jobenv :: Jobenv,
    jobhome :: String,
    jobdata :: String
}
data Jobenv = Jobenv [(String, String)] --needed it for toJSON dict conversion
instance ToJSON Jobenv where
    toJSON (Jobenv env_list) = toJSON $ Map.fromList env_list

data Header = Header {
    magic :: Word16,
    vs :: Word16,
    jobdict_offset :: Word32,
    jobenv_offset :: Word32,
    jobhome_offset :: Word32,
    jobdata_offset :: Word32
} deriving (Show)

instance Binary Header where
    put (Header m v jdo jeo jho jdto) = do
        put m
        put v
        put jdo
        put jeo
        put jho
        put jdto

    get = do
        m <- get
        v <- get
        jdo <- get
        jeo <- get
        jho <- get
        jdto <- get
        return (Header m v jdo jeo jho jdto)

make_header :: Word16 -> Int -> Int -> Int -> Header
make_header version job_dict_len job_env_len job_home_size =
    Header {
        magic = get_magic,
        vs = version,
        jobdict_offset = fromIntegral hs :: Word32,
        jobenv_offset = fromIntegral $ hs + job_dict_len :: Word32,
        jobhome_offset = fromIntegral $ hs + job_dict_len + job_env_len :: Word32,
        jobdata_offset = fromIntegral $ hs + job_dict_len + job_env_len + job_home_size :: Word32
    }
    where
        hs = get_header_size

data Jobinput = Jobinput {
    label :: Int,
    size_hint :: Int,
    url_locs :: [String]
}

instance ToJSON Jobinput where
    toJSON (Jobinput l sh urls) = toJSON (l, sh, urls)

--TODO no
data Jobdict = Jobdict_vs1 {
                owner1 :: String,
                worker1 :: String, --the path to the worker binary relative to the jobhome
                prefix1 :: String,
                has_map :: Bool,
                has_reduce :: Bool,
                nr_reduces :: Int,
                scheduler :: String, --max_cores, force_local, force_remote
                inputs1 :: [[String]], -- TODO make it work for multiple replicas
                save_results :: Bool,
                save_info :: String
            }
            | Jobdict_vs2 {
                owner2 :: String,
                worker2 :: String,
                prefix2 :: String,
                pipeline :: [(String, Grouping)], --(stage, grouping)
                inputs2 :: [(Int, Int, [String])],
                save_results :: Bool,
                save_info :: String
            }


instance ToJSON Jobdict where
    toJSON (Jobdict_vs1 o w p hm hr nr s i sr si) = object ["owner" .= o,
                                                        "worker" .= w,
                                                        "prefix" .= p, 
                                                        "map?" .= hm, 
                                                        "reduce?" .= hr, 
                                                        "nr_reduces" .= nr, 
                                                        "scheduler" .= s,
                                                        "input" .= i, 
                                                        "save_reults" .= sr,
                                                        "save_info" .= si
                                                ]
    toJSON (Jobdict_vs2 o w p pipe i sr si) = object ["owner" .= o,
                                                    "worker" .= w,
                                                    "prefix" .= p, 
                                                    "pipeline" .= pipe,
                                                    "input" .= i, 
                                                    "save_reults" .= sr,
                                                    "save_info" .= si
                                                ]


instance ToJSON Grouping where
    toJSON Split = String "split"
    toJSON Group_label = String "group_label"
    toJSON Group_node = String "group_node"
    toJSON Group_node_label = String "group_node_label"
    toJSON Group_all = String "group_all"

get_owner :: IO String
get_owner = do
    usr <- getEffectiveUserName
    host <- getHostName
    return $ usr ++ "@" ++ host

create_classic_jobpack :: [String] -> FilePath -> IO ()
create_classic_jobpack inputs worker_path = do
    own <- get_owner
    inp <- get_effective_inputs inputs
    let jd1 = Jobdict_vs1 {
            owner1 = own,
            worker1 = "./job",
            prefix1 = "hjob_classic",
            has_map = True,
            has_reduce = True,
            nr_reduces = 1,
            scheduler = "",
            inputs1 = inp,
            save_results = False,
            save_info = "ddfs"
        } 
    let jp = Jobpack jd1 (Jobenv []) [] [] --TODO it's hardcoded change that
    zip_encode_jp jp worker_path get_version1

create_pipeline_jobpack :: [(String, Grouping)] -> [(Int,Int,String)] -> FilePath -> IO ()
create_pipeline_jobpack pipeln inputs worker_path = do
    own <- get_owner
    let (inpt_labels, inpt_sizes , inpt_urls) = unzip3 inputs
    effective_urls <- get_effective_inputs inpt_urls
    let jobinpts = zip3 inpt_labels inpt_sizes effective_urls
    let jd2 = Jobdict_vs2 {
            owner2 = own,
            worker2 = "./job",
            prefix2 = "hjob_pipeline",
            pipeline = pipeln,
            inputs2 = jobinpts,
            save_results = False,
            save_info = "ddfs"
        }
    let jp = Jobpack jd2 (Jobenv []) [] [] --TODO it's hardcoded change that
    zip_encode_jp jp worker_path get_version2
        
zip_encode_jp :: Jobpack -> FilePath -> Word16 -> IO ()
zip_encode_jp jp worker_exe version = do
    let jd = B.encode $ Data.Aeson.encode $ jobdict jp
    let je = B.encode $ Data.Aeson.encode $ jobenv jp
    let jd_len = fromIntegral $ BL.length jd
    let je_len = fromIntegral $ BL.length je
    putStrLn $ BL.unpack $ Data.Aeson.encode $ jobdict jp
    --TODO change it -> need create zip archive from Haskell
    exe_contents <- BL.readFile $ worker_exe ++ ".zip" --TODO hack hardcoded 
--    let zip_compr = compress exe_contents
    jhome_size <- getFileStatus (worker_exe ++ ".zip") >>= \s -> return $ fileSize s
--    let header = make_header version jd_len je_len (fromIntegral $ BL.length zip_compr)
    let header = make_header version jd_len je_len (fromIntegral $ jhome_size)
    putStrLn $ show header
    withFile "jp" WriteMode $ \h ->  writeBins header jd je exe_contents h

writeBins :: Header -> BL.ByteString -> BL.ByteString -> BL.ByteString -> Handle -> IO ()
writeBins header jd je zip_compr handle = do
    let bl = B.encode $ (replicate 25 0 :: [Word32]) --TODO
    let encoded_header = BL.append (B.encode header) bl
    let file_prefix = BL.append encoded_header $ BL.append jd je
    let whole_bytestr = BL.append file_prefix (B.encode zip_compr)
    putStrLn $ show $ BL.length whole_bytestr
    BL.hPut handle whole_bytestr

cleanup :: IO ()
cleanup = do 
    try (removeFile "jp") :: IO (Either IOException ())
    return ()

submit_job :: IO String
submit_job = do
    let Just addr = parseURI "http://localhost:8989/disco/job/new" --TODO hardcoded
    contents <- BL.readFile "jp" --TODO check if exist, check if it is done reading etc.
    jp_size <- getFileStatus "jp" >>= \s -> return $ fileSize s
    let request = Http.Request {Http.rqURI = addr,
                             Http.rqMethod = Http.POST,
                             Http.rqHeaders = [Http.mkHeader Http.HdrContentType "image/jpg",
                                            Http.mkHeader Http.HdrContentLength (show jp_size)
                                   ],
                             Http.rqBody = BL.unpack contents}
    Http.simpleHTTP request >>= Http.getResponseBody


--main = do
--    create_pipeline_jobpack [("map", Split)] [(0,0,"tag://data:more_chek"), (1,1,"http://cos")] "word_count"
--    submit
--    cleanup

