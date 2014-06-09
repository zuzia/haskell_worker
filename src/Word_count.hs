module Main
    where

import Worker
import Protocol
import Data.List
import System.IO
import Data.Function

-- TODO - make an interface to implement
-- basic interface, types of map and reduce
-- TODO types
kvcompare :: (Key, Value) -> (Key, Value) -> Bool
kvcompare (k1,_) (k2, _) = k1==k2

kvgroup ::[(Key, Value)] -> [[(Key, Value)]]
kvgroup l = groupBy kvcompare (sortBy (compare `on` fst) l)

my_map :: Map
my_map line =
    [(x,1) | x <- words line]

--TODO
map_reader :: Process
map_reader fileContents out_handle = do 
--    fileContents <- hGetContents in_handle
    let calc_out = concatMap my_map (lines fileContents)
    hPrint out_handle calc_out

--TODO
reduce_reader :: Process
reduce_reader fileContents out_handle = do 
    let calc_out = my_reduce (read fileContents :: [(Key, Value)])
    hPrint out_handle calc_out


my_reduce :: Reduce
my_reduce iter =
    map (helper_func 0) (kvgroup iter)
-- it is fold for summing (key,value) lists with the same key in tuple
helper_func :: Int -> [(Key, Value)] -> (Key, Value)
helper_func acc [(x,y)] = (x,acc+y)
helper_func acc ((x,y):xs) = helper_func (acc+y) xs

naive_test_protocol :: IO ()
naive_test_protocol = do
    send_worker
    Just M_ok <- recive
    Just (M_task task) <- exchange_msg W_task
    Just (M_task_input t_input) <- exchange_msg $ W_input Empty
    let output = Output 0 "sth" 1000
    Just M_ok <- exchange_msg $ W_output output
    Just M_ok <- exchange_msg W_done
    return ()

main :: IO ()
main = do
--    naive_test_protocol
    Worker.run map_reader reduce_reader
    return ()

