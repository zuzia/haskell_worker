module Worker
    where

import Protocol
import Reader
import Data.List
import System.Directory
import System.IO

type Map = String -> [(Key, Value)]
type Reduce = [(Key, Value)] -> [(Key, Value)]
type Key = String
type Value = Int

--map, reduce reader gathered type
--type Process = Handle -> Handle -> IO ()
type Process = String -> Handle -> IO ()
--TODO made it just to work for now, later extract reader part

run_stage :: FilePath -> String -> Task -> Task_input -> Process -> IO (Maybe Output)
run_stage pwd file_templ task t_input process_fun = do  
--prepare directory for outputs temp
    (tempFilePath, tempHandle) <- openTempFile pwd file_templ

    let locations =  map replica_location $ concatMap replicas (inputs t_input)
    --TODO multiple inputs, multiple replica's locations
    --TODO it's written just to work for now, head hack
    putStrLn $ "locations " ++ show(locations)
    process_input <- address_reader (head locations) task
    process_fun process_input tempHandle
    let out_path = disco_output_path tempFilePath task
    out_size <- hFileSize tempHandle 
    hClose tempHandle
    return $ Just (Output 0 out_path out_size)

--TODO pretty nasty hard-coded
shuffle_out :: Task_input -> IO (Maybe Output)
shuffle_out t_input =
    return $ Just (Output lab loc 0)
    where
        inpt = head $ inputs t_input
        repl = head $ replicas inpt
        loc = replica_location repl
        lab = input_label inpt

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
    --nein
    Just output <- case stage task of
        Map_shuffle -> shuffle_out t_input 
        _ -> do run_stage pwd file_templ task t_input process_fun --TODO
    Just M_ok <- exchange_msg $ W_output output --TODO multiple outputs
    Just M_ok <- exchange_msg W_done
    return ()

