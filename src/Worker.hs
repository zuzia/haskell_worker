module Worker
    where

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

--TODO shuffling -> python classic worker, functions:
--shuffle
--shuffle group node

-----------------------------------------------------------------
inpt_compare :: Input -> Input -> Bool
inpt_compare inp1 inp2 = (input_label inp1) == (input_label inp2)

inpt_group_label :: [Input] -> [[Input]]
inpt_group_label = groupBy inpt_compare

group_inputs :: [Input] -> Task -> [[Input]]
group_inputs inpts_list task=
    case (grouping task) of
        "split" -> [[x] | x <- inpts_list]
        "group_all" -> [inpts_list]
        "group_label" -> inpt_group_label inpts_list
        "group_node" -> [inpts_list] --TODO
        "group_node_label" -> [inpts_list] --TODO

-----------------------------------------------------------------

get_locations :: [Input] -> [String]
get_locations inpts =
    map replica_location $ map (head . replicas) inpts --TODO I take just first replica location

run_process_function :: Process -> FilePath -> String -> [String] -> Task -> IO [Maybe Output]
run_process_function process_fun pwd file_templ inpt_list task = do
    (tempFilePath, tempHandle) <- openTempFile pwd file_templ
    process_fun inpt_list tempHandle
    let out_path = disco_output_path tempFilePath task
    out_size <- hFileSize tempHandle
    hClose tempHandle
    return $ [Just (Output 0 out_path out_size)] --TODO labels?

run_stage :: FilePath -> String -> Task -> [Input] -> Process -> IO [Maybe Output]
run_stage pwd file_templ task inpts process_fun = do  
    let locs = get_locations inpts
--    exchange_msg $ W_msg ("locs" ++ show(length locs))
    read_inpts <- read_inputs task locs
    run_process_function process_fun pwd file_templ read_inpts task

--TODO change it
shuffle_out :: [Input] -> IO [Maybe Output]
shuffle_out inpts =
    return $ map (\(Input _ _ lab repls) -> Just (Output lab (loc repls) 0)) inpts
    where
        loc = \xs -> replica_location (head xs) --TODO just first replica location

--iterate over the list of outputs
send_outputs :: (Maybe Output) -> MaybeT IO Master_msg
send_outputs output = do
    case output of 
        Just out -> do exchange_msg $ W_output out
        Nothing -> mzero --TODO

--polling for inputs, until done flag
--get_inputs :: [Int] -> IO [Input]
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

get_stage_tmp :: Task -> Process -> Process -> (String, Process)
get_stage_tmp task map_fun reduce_fun =
    case stage task of
        "map" -> ("map_out_", map_fun)
        "reduce" -> ("reduce_out_", reduce_fun)

--TODO error handling!
run :: Process -> Process -> MaybeT IO ()
run map_fun reduce_fun = do
    pwd <- lift getCurrentDirectory
    lift send_worker
    recive --TODO M_ok in recive
    M_task task <- exchange_msg W_task
    let (file_templ, process_fun) = get_stage_tmp task map_fun reduce_fun
    M_task_input t_input <- exchange_msg $ W_input Empty
    inputs_list <-  get_inputs []
    outputs <- case stage task of
        "map_shuffle" -> lift $ shuffle_out inputs_list
        _ -> do lift $ run_stage pwd file_templ task inputs_list process_fun
    mapM send_outputs outputs >> exchange_msg W_done >> return ()

--TODO errors, and maybe process
run_worker :: Process -> Process -> IO ()
run_worker map_fun reduce_fun = do
    result <- runMaybeT $ run map_fun reduce_fun --TODO Nothing + wrap it
    case result of
        Nothing -> (runMaybeT $ exchange_msg $ W_fatal "Protocol error") >> return ()
        Just _ -> return ()

