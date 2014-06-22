module Worker
    where

import Protocol
import Reader
import Data.List
import System.Directory
import System.IO
import Control.Exception (handle, fromException, IOException)

type Map = String -> [(Key, Value)]
type Reduce = [(Key, Value)] -> [(Key, Value)]
type Key = String
type Value = Int

--map, reduce reader gathered type
--TODO rewrite it to StringIO
type Process = [String] -> Handle -> IO ()

inpt_compare :: Input -> Input -> Bool
inpt_compare inp1 inp2 = (input_label inp1) == (input_label inp2)

inpt_group_label :: [Input] -> [[Input]]
inpt_group_label = groupBy inpt_compare

group_inputs :: Task_input -> Task -> [[Input]]
group_inputs t_input task=
    case (grouping task) of
        "split" -> [[x] | x <- inpts_list]
        "group_all" -> [inpts_list]
        "group_label" -> inpt_group_label inpts_list
        "group_node" -> [inpts_list] --TODO
        "group_node_label" -> [inpts_list] --TODO
    where inpts_list = inputs t_input

get_locations :: [[Input]] -> [[String]]
get_locations grouped_inpts =
    map locs grouped_inpts
    where locs inps = map replica_location $ map (head . replicas) inps --TODO I take just first replica location

run_process_function :: Process -> FilePath -> String -> [String] -> Task -> IO (Maybe Output)
run_process_function process_fun pwd file_templ inpt_list task = do
    (tempFilePath, tempHandle) <- openTempFile pwd file_templ
    process_fun inpt_list tempHandle
    let out_path = disco_output_path tempFilePath task
    out_size <- hFileSize tempHandle
    hClose tempHandle
    return $ Just (Output 0 out_path out_size) --TODO labels?

run_stage :: FilePath -> String -> Task -> Task_input -> Process -> IO [Maybe Output]
run_stage pwd file_templ task t_input process_fun = do  
    let grouped_locs = get_locations $ group_inputs t_input task
    read_inpts <- mapM (\locs -> read_inputs task locs) grouped_locs
    mapM (\inpt_list -> run_process_function process_fun pwd file_templ inpt_list task) read_inpts --returns list of Maybe Outputs

--TODO change it
shuffle_out :: Task_input -> IO [Maybe Output]
shuffle_out t_input =
    return $ map (\(Input _ _ lab repls) -> Just (Output lab (loc repls) 0)) inpts
    where
        inpts = inputs t_input --TODO grouping
        loc = \xs -> replica_location (head xs) --TODO just first replica location

--iterate over the list of outputs
send_outputs :: (Maybe Output) -> IO (Maybe Master_msg)
send_outputs output = do
    case output of 
        Just out -> do exchange_msg $ W_output out
        Nothing -> do return Nothing --TODO

--TODO error handling!
run :: Process -> Process -> IO ()
run map_fun reduce_fun = do
    send_worker
    Just M_ok <- recive
    Just (M_task task) <- exchange_msg W_task
    pwd <- getCurrentDirectory
    let (file_templ, process_fun) = case stage task of
            Map -> ("map_out_", map_fun)
            Reduce -> ("reduce_out_", reduce_fun)
    Just (M_task_input t_input) <- exchange_msg $ W_input Empty
    outputs <- case stage task of
        Map_shuffle -> shuffle_out t_input 
        _ -> do run_stage pwd file_templ task t_input process_fun
    mapM send_outputs outputs
    Just M_ok <- exchange_msg W_done
    return ()

