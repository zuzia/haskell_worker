module Pipeline
    where

import Protocol
import Reader
import qualified Worker as W
import qualified Data.List as DL
import System.Directory (getCurrentDirectory)
import Control.Monad (liftM)
import Control.Exception (handle)

--TODO functions in class Stage;
--input chain, output chain
--input_hook - allows user to specify the order in which the input labels should be iterated over
--done - function called after all the processing

-- The order of invocation of the task entry points of a stage
-- are: *input_hook*, *init*, *process*, and *done*, where *init* and
-- *done* are optional and called only once, while *process* is called once for every task input.

data Grouping
    = Split
    | Group_label
    | Group_node
    | Group_node_label
    | Group_all

data Stage = Stage {
    process_fun :: W.Process,
    name :: String,
    grouping :: Grouping
}
--    init ::
--    done ::
--    combine ::
--    sort ::
--    input_chain ::
--    output_chain ::
type Pipeline = [Stage]

stage_names :: Pipeline -> [String]
stage_names pipeline =
    map name pipeline

get_process_fun :: Pipeline -> String -> Maybe W.Process
get_process_fun pipeline stage_name =
    DL.lookup stage_name assoc
    where assoc = map (\p -> (name p, process_fun p)) pipeline

run_task :: FilePath -> String -> Task -> [Input] -> W.Process -> IO (Maybe Master_msg)
run_task pwd file_templ task list_inputs process_fun = do
    outputs <- W.run_stage pwd file_templ task list_inputs process_fun
    mapM W.send_outputs outputs
    exchange_msg W_done
    
--analogous to classic run
run :: Pipeline -> IO ()
run pipeline = do
    send_worker
    Just M_ok <- recive
    Just (M_task task) <- exchange_msg W_task
    pwd <- getCurrentDirectory
    let task_stage = stage task
    let file_templ = task_stage ++ "_out_"
    --Just (M_task_input t_input) <- exchange_msg $ W_input Empty
    inputs_list <-  W.get_inputs []
    case (get_process_fun pipeline task_stage) of
        Just process_fun -> do run_task pwd file_templ task inputs_list process_fun
        Nothing -> do exchange_msg $ W_fatal "Non-existent stage"
    --Just M_ok <- exchange_msg W_done
    return ()

--for example pipeline = [("map", Split), ("shuffle", Group_node), ("reduce", Group_all)]
start_pipeline :: Pipeline -> IO () 
start_pipeline pipeline =
    --handle error_wrapper (run pipeline) --TODO error wrapper
    run pipeline

