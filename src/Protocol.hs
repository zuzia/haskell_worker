module Protocol
    where
import System.IO
--module written according to worker protocol section in documentation
--TODO JSON parser
--TODO write functions

-- messages from the Worker to Disco
-- WORKER message, announce the startup of the worker.
data Worker_info = Worker_info {
    version :: String, --"1.0"
    pid :: Int
}
 
-- TASK message, Worker sends it without payload
-- request the task information from disco
data Task_stage = Map | Reduce

data Task = Task {
    host :: String,
    master :: String,
    jobname :: String,
    task_id :: Int,
    stage :: Task_stage, --map, reduce
    grouping :: String, -- not in docs TODO pipeline
    group :: String, -- not in docs TODO pipeline
    disco_port :: Int, -- DISCO_PORT
    put_port :: Int, -- DDFS_PUT_PORT
    disco_data :: String, -- DISCO_DATA
    ddfs_data :: String, -- DDFS_DATA
    jobfile :: String -- path to the job pack file
}

-- ocaml: group_label, group_node

-- INPUT message, Worker sends it without payload
-- or exclude, include
-- Request input for the task from Disco.
-- TODO what about flags: "more", "done"?

data Replica = Replica {
    replica_id :: Int,
    replica_location :: String
}
--TODO maybe use it in replica location
--data Scheme 
--  = Dir
--  | Disco
--  | File
--  | Raw
--  | Http
--  | Other String

data Input_flag = More | Done
data Input_status = Ok | Busy | Failed

data Input = Input {
    input_id :: Int,
    status :: Input_status, -- ok, busy, failed
    input_label :: Int, -- TODO not in docs, pipeline again
    replicas :: [Replica]
}

data Worker_input_msg = Exclude Int | Include Int | Empty

data Input_err = Input_err {
    input_err_id :: Int,
    rep_ids :: [Int]	
}

data Output_type = Disco | Part | Tag

data Output = Output {
    output_label :: String,
    output_location :: String,
    output_size :: Int, -- TODO
    output_type :: Output_type
}

--TODO maybe other solution
data Worker = Worker {
    task :: Task,
    inputs :: [Input],
    outputs :: [Output]
}

-- looks ugly, change names?
data Worker_msg
    = W_worker Worker_info
    | W_task --no payload
    | W_input Worker_input_msg
    | W_input_err Input_err
    | W_msg String
    | W_output Output
    | W_done
    | W_error String
    | W_fatal String
    | W_ping

-- messages from Disco to Worker
data Master_msg 
    = M_ok
    | M_die
    | M_task Task
    | M_task_input Input
    | M_retry [Replica]
    | M_fail
    | M_wait Int

-- TODO functions
-- init_worker
-- request_task
-- handle_task
-- request_input
-- handle_input
-- prepare_msg JSON

--request_done

