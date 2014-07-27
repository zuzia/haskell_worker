module Pipeline
    where

import Protocol
import Reader
import Task as T
import qualified Worker as W
import qualified Data.List as DL
import System.Directory (getCurrentDirectory)
import Control.Monad
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe
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
    process_fun :: T.Process,
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

get_process_fun :: Pipeline -> String -> Maybe T.Process
get_process_fun pipeline stage_name =
    DL.lookup stage_name assoc
    where assoc = map (\p -> (name p, process_fun p)) pipeline

run_task :: FilePath -> String -> Task -> [Input] -> T.Process -> MaybeT IO Master_msg
run_task pwd file_templ task list_inputs process_fun = do
    -- TODO call init function
    -- TODO call done function
    outputs <- lift $ W.run_stage pwd file_templ task list_inputs process_fun --check combine, sort flags + input_hook fun
    mapM W.send_outputs outputs
    exchange_msg W_done
    
--analogous to classic run
run :: Pipeline -> MaybeT IO ()
run pipeline = do
    pwd <- lift getCurrentDirectory
    lift send_worker
    W.expect_ok
    M_task task <- exchange_msg W_task
    let task_stage = stage task
    let file_templ = task_stage ++ "_out_"
    inputs_list <-  W.get_inputs []
    case (get_process_fun pipeline task_stage) of
        Just process_fun -> do run_task pwd file_templ task inputs_list process_fun
        Nothing -> do exchange_msg $ W_fatal "Non-existent stage"
    return ()

--for example pipeline = [("map", Split), ("shuffle", Group_node), ("reduce", Group_all)]
start_pipeline :: Pipeline -> IO () 
start_pipeline pipeline = do
    --handle error_wrapper (run pipeline) --TODO error wrapper
    result <- runMaybeT $ run pipeline
    case result of
        Nothing -> (runMaybeT $ exchange_msg $ W_fatal "Protocol error") >> return ()
        Just _ -> return ()

