{-# LANGUAGE OverloadedStrings #-}

module Protocol
    where

import System.IO
import System.Posix.Process
import Data.Aeson
import qualified Data.Vector as V
import Data.Maybe
import Control.Applicative ((<$>), (<*>), empty)
import Control.Monad
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.Text as DT

-- messages from the Worker to Disco
get_version :: String
get_version = "1.1"

timeout = 600 -- in seconds

data Worker_info = Worker_info {
    version :: String,
    pid :: Int
} deriving (Show, Eq)

instance ToJSON Worker_info where
     toJSON (Worker_info version pid) = object ["version" .= version, "pid" .= pid]

-- TASK message, Worker sends it without payload
-- request the task information from disco
data Task = Task {
    taskid :: Int,
    master :: String,
    disco_port :: Int,
    put_port :: Int,
    ddfs_data :: String,
    disco_data :: String,
    stage :: String,
    grouping :: String,
    group :: (Int,String),
    jobfile :: String,
    jobname :: String,
    host :: String
} deriving (Show, Eq)

instance FromJSON Task where
    parseJSON (Object v) =
        Task <$>
        (v .: "taskid") <*>
        (v .: "master") <*>
        (v .: "disco_port") <*>
        (v .: "put_port") <*>
        (v .: "ddfs_data") <*>
        (v .: "disco_data") <*>
        (v .: "stage") <*>
        (v .: "grouping") <*>
        (v .: "group") <*>
        (v .: "jobfile") <*>
        (v .: "jobname") <*>
        (v .: "host")
    parseJSON _ = empty

-- INPUT message, Worker sends it without payload
-- or with exclude, include
-- Request input for the task from Disco.

data Replica = Replica {
    replica_id :: Int,
    replica_location :: String
} deriving (Show, Eq)

instance FromJSON Replica where
    parseJSON (Array v) = 
        case (V.length v) of
            2 -> let rid:rloc:_ = V.toList v in
                    Replica <$>
                    (parseJSON rid) <*>
                    (parseJSON rloc)
            otherwise -> empty
    parseJSON _ = empty

data Input_flag = More | Done deriving (Show, Eq) --kept in M_task_input
data Input_status = Ok | Busy | Failed deriving (Show, Eq)

instance FromJSON Input_flag where
    parseJSON (String iflag) =
        case parse_input_flag (DT.unpack iflag) of
            Just fl -> return fl
            Nothing -> empty
    parseJSON _ = empty

parse_input_flag :: String -> Maybe Input_flag
parse_input_flag iflag =
    case iflag of
        "more" -> Just More
        "done" -> Just Done
        otherwise -> Nothing

parse_input_status :: String -> Maybe Input_status
parse_input_status istat =
    case istat of
        "ok" -> Just Ok
        "busy" -> Just Busy
        "failed" -> Just Failed
        otherwise -> Nothing

instance FromJSON Input_status where
    parseJSON (String istat) =
        case parse_input_status (DT.unpack istat) of
            Just st -> return st
            Nothing -> empty
    parseJSON _ = empty

data Input = Input {
    input_id :: Int,
    status :: Input_status, -- ok, busy, failed
    input_label :: Int,
    replicas :: [Replica]
} deriving (Show, Eq)

instance FromJSON Input where
    parseJSON (Array v) = 
        case (V.length v) of
            4 ->
                let iid:stat:lab:rep:_ = V.toList v in
                Input <$>
                (parseJSON iid) <*>
                (parseJSON stat) <*>
                (parseJSON lab) <*>
                (parseJSON rep)
            otherwise -> empty
    parseJSON _ = empty

data Task_input = Task_input {
    input_flag :: Input_flag,
    inputs :: [Input]
} deriving (Show, Eq)

instance FromJSON Task_input where
    parseJSON (Array v) =
        case (V.length v) of
            2 ->
                let t_flag = V.head v
                    [ins] = (V.toList . V.tail) v in
                Task_input <$>
                parseJSON t_flag <*>
                parseJSON ins
            otherwise -> empty
    parseJSON _ = empty

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
    output_size :: Integer
--    output_type :: Output_type
} deriving (Show, Eq)

--TODO maybe other solution
--data Worker = Worker {
--    task :: Task,
--    inputs :: [Input],
--    outputs :: [Output]
--} deriving (Show, Eq)

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
    | M_task_input Task_input
    | M_retry [Replica]
    | M_fail
    | M_wait Int deriving (Show, Eq)

-- MSG format: <name> ‘SP’ <payload-len> ‘SP’ <payload> ‘\n’
-- 'SP' - single space character
-- <payload-len> - is the length of the <payload> in bytes
-- <payload> is a JSON formatted term

prep_init_worker :: IO BL.ByteString
prep_init_worker = getProcessID >>= json_w_info
    where json_w_info pid = return $ encode Worker_info{version = get_version, pid = fromIntegral pid}

prep_input_msg :: Worker_input_msg -> BL.ByteString
prep_input_msg wim =
    case wim of
        Exclude xs -> encode (String "exclude", xs)
        Include xs -> encode (String "include", xs)
        Empty -> encode $ String ""

prep_input_err :: Input_err -> BL.ByteString
prep_input_err (Input_err ie_id xs) = encode (ie_id, xs)

prep_output :: Output -> BL.ByteString 
prep_output (Output label location o_size) = encode (label, location, o_size)

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
send_worker :: IO ()
send_worker = prep_init_worker >>= send1
    where send1 json_msg = hPutStrLn stderr $ unwords ["WORKER", show (BL.length json_msg), BL.unpack json_msg]

send :: Worker_msg -> IO ()
send wm = do
    let (tag, json_msg) = prepare_msg wm
    hPutStrLn stderr $ unwords [tag, show (BL.length json_msg), BL.unpack json_msg]

--TODO wait until timeout!
recive :: MaybeT IO Master_msg
--recive :: IO (Maybe Master_msg)
recive = do
    in_msg  <- lift getLine
    let [msg, payload_len, payload] = words in_msg
    --return $ process_master_msg msg payload --lift?
    process_master_msg msg payload

-- synchronized message exchange
exchange_msg :: Worker_msg -> MaybeT IO Master_msg
--exchange_msg :: Worker_msg -> IO (Maybe Master_msg)
exchange_msg wm = do
    lift $ send wm
    lift $ hFlush stderr
    recive

process_master_msg :: MonadPlus m => String -> String -> m Master_msg
--process_master_msg :: String -> String -> Maybe Master_msg
process_master_msg msg payload = 
    case msg of
        "OK" -> return M_ok
        "DIE" -> return M_die
        --"TASK" -> fmap M_task $ (decode . BL.pack) payload --TODO
        "TASK" -> case ((decode . BL.pack) payload :: Maybe Task) of
                    Nothing -> mzero
                    Just t -> return $ M_task t
        "FAIL" -> return M_fail
        --"RETRY" -> fmap M_retry (((decode . BL.pack) payload) :: Maybe [Replica]) --TODO
        "RETRY" -> case (((decode . BL.pack) payload) :: Maybe [Replica]) of
                    Nothing -> mzero
                    Just r -> return $ M_retry r
        "WAIT" -> return $ M_wait $ read payload
        --"INPUT" -> fmap M_task_input ((decode . BL.pack) payload :: Maybe Task_input) --TODO
        "INPUT" -> case ((decode . BL.pack) payload :: Maybe Task_input) of
                    Nothing -> mzero
                    Just ti -> return $ M_task_input ti
        otherwise -> mzero

