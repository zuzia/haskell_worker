{-# LANGUAGE OverloadedStrings #-}

module Protocol
    where

import System.IO
import System.Posix.Process
import Data.Aeson
import Control.Applicative ((<$>), (<*>), empty)
import Control.Monad (liftM)
import qualified Data.ByteString.Lazy.Char8 as BL

--module written according to worker protocol section in documentation

-- messages from the Worker to Disco
-- WORKER message, announce the startup of the worker.
version :: String
get_version = "1.1"

timeout = 600 -- in seconds

data Worker_info = Worker_info {
    version :: String, --"1.1"
    pid :: Int
} deriving (Show, Eq)

instance ToJSON Worker_info where
     toJSON (Worker_info version pid) = object ["version" .= version, "pid" .= pid]

-- TASK message, Worker sends it without payload
-- request the task information from disco
data Task_stage = Map | Reduce deriving (Show, Eq)

instance ToJSON Task_stage where
    toJSON Map = String "map"
    toJSON Reduce = String "reduce"

data Task = Task {
    host :: String,
    master :: String,
    jobname :: String,
    task_id :: Int,
    stage :: Task_stage, --map, reduce
--    grouping :: String, -- not in docs TODO pipeline
--    group :: String, -- not in docs TODO pipeline
    disco_port :: Int, -- DISCO_PORT
    put_port :: Int, -- DDFS_PUT_PORT
    disco_data :: String, -- DISCO_DATA
    ddfs_data :: String, -- DDFS_DATA
    jobfile :: String -- path to the job pack file
} deriving (Show, Eq)

parse_task_stage :: String -> Task_stage
parse_task_stage ts =
    case ts of
        "map" -> Map
        "reduce" -> Reduce
--TODO ----------------------------------------------------------------------------------
instance FromJSON Task where
    parseJSON (Object v) =
        Task <$>
        (v .: "host") <*>
        (v .: "master") <*>
        (v .: "jobname") <*>
        (v .: "task_id") <*>
        liftM parse_task_stage (v .: "stage") <*>
        (v .: "disco_port") <*>
        (v .: "put_port") <*>
        (v .: "disco_data") <*>
        (v .: "ddfs_data") <*>
        (v .: "jobfile")
    parseJSON _ = empty
-----------------------------------------------------------------------------------------
-- ocaml: group_label, group_node

-- INPUT message, Worker sends it without payload
-- or exclude, include
-- Request input for the task from Disco.
-- TODO what about flags: "more", "done"?

data Replica = Replica {
    replica_id :: Int,
    replica_location :: String
} deriving (Show, Eq)
--TODO DISCO RETRY MSG ------------------------------------------------------------------
instance FromJSON Replica where
    parseJSON (Object v) =
        Replica <$>
        (v .: "replica_id") <*> --TODO not exactly, think about it
        (v .: "replica_location")
    parseJSON _ = empty
-----------------------------------------------------------------------------------------

--TODO maybe use it in replica location
--data Scheme 
--  = Dir
--  | Disco
--  | File
--  | Raw
--  | Http
--  | Other String

data Input_flag = More | Done deriving (Show, Eq) --kept in M_task_input
data Input_status = Ok | Busy | Failed deriving (Show, Eq)

parse_input_flag :: String -> Input_flag
parse_input_flag iflag =
    case iflag of
        "more" -> More
        "done" -> Done

parse_input_status :: String -> Input_status
parse_input_status istat =
    case istat of
        "ok" -> Ok
        "busy" -> Busy
        "failed" -> Failed

data Input = Input {
    input_id :: Int,
    status :: Input_status, -- ok, busy, failed
--    input_label :: Int, -- TODO not in docs, pipeline again
    replicas :: [Replica]
} deriving (Show, Eq)
--TODO ----------------------------------------------------------------------------------
instance FromJSON Input where
    parseJSON (Object v) =
        Input <$>
        (v .: "input_id") <*>
        liftM parse_input_status (v .: "status") <*>
        (v .: "replicas")--TODO nested type
    parseJSON _ = empty
-----------------------------------------------------------------------------------------
--

--['exclude', [input_id]] or ['include', [input_id]] or ""
data Worker_input_msg = Exclude [Int] | Include [Int] | Empty deriving (Show, Eq)

--[input_id, [rep_id]]
data Input_err = Input_err {
    input_err_id :: Int,
    rep_ids :: [Int]	
} deriving (Show, Eq)

data Output_type = Disco | Part | Tag deriving (Show, Eq)
instance ToJSON Output_type where
    toJSON Disco = String "disco"
    toJSON Part = String "part"
    toJSON Tag = String "tag"

--[output_location, output_type, label]
data Output = Output {
    output_label :: Int, --TODO check type
    output_location :: String,
--    output_size :: Int, -- TODO
    output_type :: Output_type
} deriving (Show, Eq)

--TODO maybe other solution
data Worker = Worker {
    task :: Task,
    inputs :: [Input],
    outputs :: [Output]
} deriving (Show, Eq)

data Worker_msg
     = W_worker
    | W_task
    | W_input Worker_input_msg
    | W_input_err Input_err
    | W_msg String
    | W_output Output
    | W_done
    | W_error String
    | W_fatal String
    | W_ping deriving (Show, Eq)

-- messages from Disco to Worker
data Master_msg 
    = M_ok
    | M_die
    | M_task Task
    | M_task_input Input_flag Input
    | M_retry [Replica]
    | M_fail
    | M_wait Int deriving (Show, Eq)

-- TODO what about configs? in a file? 
-- in other workers it's hardcoded somewhere
--
-- MSG format: <name> ‘SP’ <payload-len> ‘SP’ <payload> ‘\n’
-- 'SP' - single space character
-- <payload-len> - is the length of the <payload> in bytes
-- <payload> is a JSON formatted term

prep_init_worker :: IO BL.ByteString
prep_init_worker = getProcessID >>= json_w_info
    where json_w_info pid = return $ encode Worker_info{version = get_version, pid = fromIntegral pid}

--TODO
prep_input_msg :: Worker_input_msg -> BL.ByteString
prep_input_msg wim =
    case wim of
        Exclude xs -> encode (String "exclude", xs)
        Include xs -> encode (String "include", xs)
        Empty -> encode $ String ""

prep_input_err :: Input_err -> BL.ByteString
prep_input_err (Input_err ie_id xs) = encode (ie_id, xs)

prep_output :: Output -> BL.ByteString 
prep_output (Output label location o_type) = encode (location, o_type, label)

-- TODO write it with error handling
prepare_msg :: Worker_msg -> (String, BL.ByteString)
prepare_msg wm =
    case wm of
        W_task -> ("TASK", encode $ String "")
        W_input w_input_msg -> ("INPUT", prep_input_msg w_input_msg)
        W_input_err input_err -> ("INPUT_ERR", prep_input_err input_err)
        W_msg msg -> ("MSG", encode msg)
        W_output output -> ("OUTPUT", prep_output output)
        W_done -> ("DONE", encode $ String "")
        W_error err_msg -> ("ERROR", encode err_msg)
        W_fatal fatal_msg -> ("FATAL", encode fatal_msg)
        W_ping -> ("PING", encode $ String "")
--        otherwise -> ("ERROR", encode $ String "Pattern matching fail, prepare_msg") --TODO

--separated because of impure getProcessPID in prepare init worker
--TODO think about other nicer solutions
send_worker :: IO ()
send_worker = prep_init_worker >>= send1
    where send1 json_msg = hPutStrLn stderr $ unwords ["WORKER", show (BL.length json_msg), BL.unpack json_msg]

send :: Worker_msg -> IO ()
send wm = do
    let (tag, json_msg) = prepare_msg wm
    hPutStrLn stderr $ unwords [tag, show (BL.length json_msg), BL.unpack json_msg]

-- TODO not gave much thougth to it, this function  will change
-- TODO buffering
recive :: IO (Maybe Master_msg)
recive = do
    hSetBuffering stdin LineBuffering
    in_msg  <- hGetLine stdin
    let [msg, payload_len, payload] = words in_msg
    return $ process_master_msg msg payload
--TODO
-- synchronized message exchange
exchange_msg :: Worker_msg -> IO (Maybe Master_msg)
exchange_msg wm = do
    send wm
    hFlush stdout
    recive

--TODO it's a lie, this function won't look like this (where condition), because I need split the message while reciving it
process_master_msg :: String -> String -> Maybe Master_msg
process_master_msg msg payload = 
    case msg of
        "OK" -> Just M_ok
        "DIE" -> Just M_die
        "TASK" -> fmap M_task $ (decode . BL.pack) payload
        "FAIL" -> Just M_fail
        "RETRY" -> fmap M_retry $ (decode . BL.pack) payload
        "WAIT" -> Just $ M_wait $ read payload --TODO errors
--        "INPUT" -> fmap M_task_input $ (decode . BL.pack) payload -- TODO input flag parsing
--    where
--        msg : payload_len : payload = words master_msg --TODO think about it, perform two breaks on space character? 

