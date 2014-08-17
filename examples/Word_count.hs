module Main
    where

import Worker
import Protocol
import System.IO
import Utils
import Task

my_map :: Num v1 => Map String p String v1
my_map line params=
    [(x,1) | x <- words line]

map_reader1 :: Process p 
map_reader1 list_inputs out_handle params= do 
    let calc_out = concatMap (\inpt -> concatMap (\l -> my_map l params) (lines inpt)) list_inputs
    mapM_ (\line -> hPrint out_handle line) calc_out

reduce_reader1 :: Process p
reduce_reader1 list_inputs out_handle params= do 
    let iterate_reduce = \inpt -> my_reduce (map (\x -> read x :: (Key, Int)) (lines inpt)) params
    let calc_out = concatMap iterate_reduce list_inputs
    mapM_ (\line -> hPrint out_handle line) calc_out

reduce_reader2 :: Process p
reduce_reader2 list_inputs out_handle params= do 
    sorted_inputs <- disk_sort list_inputs
    let calc_out = my_reduce (map (\x -> read x :: (Key, Int)) (lines sorted_inputs)) params
    mapM_ (\line -> hPrint out_handle line) calc_out

my_reduce :: Num a => Reduce String a p [(String, a)]
my_reduce iter params=
    map (helper_func 0) (kvgroup iter)

main :: IO ()
main = do
    let job = Job {
                map_fun = my_map, 
                reduce_fun = my_reduce,
                map_reader = map_reader1,
                reduce_reader = reduce_reader2,
                params = []}
    Worker.run_worker job
    return ()

