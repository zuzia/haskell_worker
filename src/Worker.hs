{-|
Module      : Worker
Description : Map Reduce only logic

This module provides an entry point to compute MapReduce task.

-}

module Worker(
    run_worker -- :: Job k1 v1 k2 v2 v3 m p -> IO ()
) where

import Protocol
import Reader
import Data.List
import System.Directory
import System.IO
import Control.Exception (handle, fromException, IOException)
import Control.Monad
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe
import Task
import Control.Concurrent (threadDelay)

----------------------------------------------------------------
--TODO tests, improvements
--Started multiple replica location handling
--
--TODO it would be nice not to block
--input_status :: Input -> MaybeT IO Input
--input_status inpt =
--    case status inpt of
--        Ok -> return inpt
--        Busy -> poll_input inpt
--        Failed -> return inpt --TODO just ignore it?
--
--poll_input :: Input -> MaybeT IO Input
--poll_input inpt = do
--    M_task_input t_input <- exchange_msg $ W_input $ Include [input_id inpt] -- TODO more, done
--    return $ (head . inputs) t_input --[Input] ?? I ask for just one input_id, but can for a list
--
----tries to read replicas in order
--read_replicas :: Task -> [Replica] -> [Replica] -> IO (Either [Replica] [String])
--read_replicas _ [] broken = return $ Left broken
--read_replicas task (r:rs) broken = do
--    read_repl <- read_dir_rest (replica_location r) task
--    case read_repl of
--        Just s -> return $ Right s
--        Nothing -> read_replicas task rs (r:broken)
--
---- TODO after this function returns Nothing -> send Error msg to disco!
--handle_mult_repl :: Task -> Input -> IO (Maybe [String])
--handle_mult_repl task inpt = do
--    let repl_list = replicas inpt
--    read_repl <- read_replicas task repl_list []
--    case read_repl of
--        Right s -> return $ Just s
--        --Left broken -> input_err inpt broken --TODO what with Nothing?
--        Left broken -> handle_again task inpt broken --TODO what with Nothing?
--
----TODO change that silly function, abstract replica processing?
--handle_again :: Task -> Input -> [Replica] -> IO (Maybe [String])
--handle_again task inpt broken = do
--    new_in <- input_err inpt broken
--    case new_in of
--        Nothing -> return Nothing
--        Just new_inpt -> do 
--            let repl_list = replicas new_inpt
--            read_repl <- read_replicas task repl_list []
--            case read_repl of
--                Right s -> return $ Just s
--                Left broken -> return Nothing
--            
--
--input_err :: Input -> [Replica] -> IO (Maybe Input)
--input_err inpt broken = do
--    let r_ids = map replica_id broken 
--    msg <- runMaybeT $ exchange_msg $ W_input_err $ Input_err (input_id inpt) r_ids
--    case msg of
--        Just (M_retry new_replicas) -> return $ Just inpt {replicas = new_replicas}
--        Just (M_wait how_long) -> (threadDelay $ how_long * 10^6) >> (runMaybeT $ poll_input inpt) --TODO what if wait repeats?
--        Just (M_fail) -> return Nothing
--        otherwise -> return Nothing

----------------------------------------------------------------

-- | Gives replicas' locations for a running task (now, for every input only one replica location is taken)
get_locations :: [Input] -> [String]
get_locations inpts =
    map replica_location $ map (head . replicas) inpts --TODO takes just first replica location

-- | This function prepares the environment for running a task's process function:
-- creates file for output, runs the task function, prepares output message
run_process_function :: Process p -> FilePath -> String -> [String] -> Task -> Job k1 v1 k2 v2 v3 m p -> IO [Maybe Output]
run_process_function process_fun pwd file_templ inpt_list task job = do
    (tempFilePath, tempHandle) <- openTempFile pwd file_templ
    process_fun inpt_list tempHandle $ params job
    let out_path = disco_output_path tempFilePath task
    out_size <- hFileSize tempHandle
    hClose tempHandle
    return $ [Just (Output (Label 0) out_path out_size)] --TODO labels?

-- | Runs stage, gets inputs' locations
run_stage :: FilePath -> String -> Task -> [Input] -> Process p -> Job k1 v1 k2 v2 v3 m p -> IO [Maybe Output]
run_stage pwd file_templ task inpts process_fun job = do  
    let locs = get_locations inpts
    read_inpts <- read_inputs task locs --TODO try
    -- TODO call init function
    run_process_function process_fun pwd file_templ read_inpts task job--check combine, sort flags + input_hook fun
    -- TODO call done function

-- | Shuffling stage
shuffle_out :: [Input] -> IO [Maybe Output]
shuffle_out inpts =
    return $ map (\(Input _ _ lab repls) -> Just (Output lab (loc repls) 0)) inpts
    where
        loc = \xs -> replica_location (head xs) --TODO just first replica location

-- | Iterates over the list of outputs and passes them to protocol handling part
-- responsible for sending output message to Disco
send_outputs :: (Maybe Output) -> MaybeT IO Master_msg
send_outputs output = do
    case output of 
        Just out -> do exchange_msg $ W_output out
        Nothing -> mzero --TODO

-- | Polling for inputs, until done flag is met
get_inputs :: [Int] -> MaybeT IO [Input]
get_inputs exclude = do
    M_task_input t_input <- exchange_msg $ W_input $ Exclude exclude
    let inpts = inputs t_input
    let excl = exclude ++ (inpt_ids t_input)
    case (input_flag t_input) of
        Done -> return inpts
        More -> liftM (inpts ++) $ get_inputs excl

inpt_ids :: Task_input -> [Int]
inpt_ids t_inpt =
    map input_id (inputs t_inpt)

-- | Helper function, returns prefix for output file and stage process function
get_stage_tmp :: Task -> Job k1 v1 k2 v2 v3 m p -> (String, Process p)
get_stage_tmp task job =
    case stage task of
        "map" -> ("map_out_", map_reader job)
        "reduce" -> ("reduce_out_", reduce_reader job)

-- | Expect OK message from Disco
expect_ok :: MaybeT IO Master_msg
expect_ok =
    recive >>= (\msg -> case msg of
                            M_ok -> return M_ok
                            _ -> mzero)

-- | Initializes a worker, sends messages in order and runs stage 
run :: Job k1 v1 k2 v2 v3 m p -> MaybeT IO ()
run job = do
    pwd <- lift getCurrentDirectory
    lift send_worker
    expect_ok
    M_task task <- exchange_msg W_task
    let (file_templ, process_fun) = get_stage_tmp task job
    inputs_list <-  get_inputs []
    outputs <- case stage task of
        "map_shuffle" -> lift $ shuffle_out inputs_list
        _ -> do lift $ run_stage pwd file_templ task inputs_list process_fun job
    mapM send_outputs outputs 
    exchange_msg W_done
    return ()

--TODO errors and maybe
-- | Entry point to whole worker logic
run_worker :: Job k1 v1 k2 v2 v3 m p -> IO ()
run_worker job = do
    result <- runMaybeT $ run job --TODO Nothing + wrap it
    case result of
        Nothing -> (runMaybeT $ exchange_msg $ W_fatal "Protocol error") >> return ()
        Just _ -> return ()

