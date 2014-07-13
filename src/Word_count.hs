module Main
    where

import Worker
import Protocol
import System.IO
import Utils
import Task

my_map :: Num a => Map a
my_map line =
    [(x,1) | x <- words line]

map_reader :: Process
map_reader list_inputs out_handle = do 
    let calc_out = concatMap (\inpt -> concatMap my_map (lines inpt)) list_inputs
    mapM_ (\line -> hPrint out_handle line) calc_out

reduce_reader :: Process
reduce_reader list_inputs out_handle = do 
    let iterate_reduce = \inpt -> my_reduce (map (\x -> read x :: (Key, Int)) (lines inpt))
    let calc_out = concatMap iterate_reduce list_inputs
    mapM_ (\line -> hPrint out_handle line) calc_out

my_reduce :: Num a => Reduce a
my_reduce iter =
    map (helper_func 0) (kvgroup iter)

main :: IO ()
main = do
    Worker.run_worker map_reader reduce_reader
    return ()

