module Main
    where

import Data.List
import Data.Function
import System.IO
import Utils
import Task
import Worker
{- Initiall implementation of kclustering algorithm, 
    now it handles only one iteration and takes initial point centers manually (need to write jobpack submission in haskell)
    Input file format: 
    (1, [1, 4])
    (2, [1, 3])
    (3, [2, 3])
    (4, [6, 1])
    (5, [7, 1])
    (6, [7, 2])
    First number is point id, second element - vector representation in d-dimensional space.
    Output:
    (1,[6.666666666666667,1.3333333333333333])
    (2,[1.3333333333333333,3.3333333333333335])
    First: cluster id, second: center coordinates
-}
data Params = Params {
    centers :: [Point],
    clusters :: Int
} deriving (Show, Eq)

data Point = Point {
    point_id::Int, 
    point_vec :: [Double] -- point representation in d-dimensional space
} deriving (Show, Eq)

reader :: [String] -> [Point]
reader list_inputs = map make_point (concatMap lines list_inputs) --TODO
    where
        make_point line = (\(p_id, p_vect) ->  Point p_id p_vect) (read line :: (Int, [Double]))

eucl_dist :: [Double] -> [Double] -> Double
eucl_dist v1 v2 = sum [(x1 - x2)^2 | (x1, x2) <- zip v1 v2]

--TODO map_init
--TODO random_init_map 

my_map :: [Point] -> Params -> [(Int,Point)] --[(cluster_id, initiall point)]
my_map points params = map (calculate_nearest params) points

-- calculates nearest center from the point
-- returns cluster id to which the point belongs to
-- TODO change that super long line
calculate_nearest :: Params -> Point -> (Int, Point)
calculate_nearest params point = (cluster_id, point)
    where
        p1 = point_vec point
        (cluster_id, _) = minimumBy (compare `on` snd) $ map (\p -> (point_id p, eucl_dist p1 $ point_vec p)) $ centers params

map_reader1 :: [String] -> Handle -> Params -> IO ()
map_reader1 list_inputs out_handle params = do
    let calc_out1 = my_map (reader list_inputs) params
    let calc_out = my_combiner calc_out1 --TODO temporary solution
    mapM_ (\line -> hPrint out_handle line) calc_out

--takes [(cluster_id, initiall point)] from map phase, and produces Key - cluster id
--value - (number of records in cluster, summed coordinates)
my_combiner :: [(Int, Point)] -> [(Int, (Int, [Double]))]
my_combiner map_data = map calc_value grouped
    where
        grouped = kvgroup map_data
        calc_value = \c -> (cluster_id c, (length c, sum_point $ snd $ unzip c))

cluster_id = \c -> (fst . head) c

sum_point :: [Point] -> [Double]
sum_point points = sum_coords $ map point_vec points

sum_coords :: [[Double]] -> [Double]
sum_coords coords = 
    foldl (zipWith (+)) (replicate dim 0) coords
    where
        dim = length $ head coords --TODO errors

my_reducer :: [(Int, (Int, [Double]))] -> Params -> [(Int, [Double])] -- cluster id, center coords
my_reducer clusters params = map calculate_center grouped 
    where
        grouped = kvgroup clusters

calculate_center :: [(Int, (Int, [Double]))] -> (Int, [Double])
calculate_center cl_group = (cluster_id cl_group, cl_center)
    where
        (number_list , cl_sums_list) = unzip $ snd $ unzip cl_group
        cl_size = sum $ number_list
        cl_center = map (/fromIntegral(cl_size)) (sum_coords cl_sums_list)

reduce_reader1 :: [String] -> Handle -> Params -> IO ()
reduce_reader1 list_inputs out_handle params = do
    let calc_out = my_reducer (aux_reader list_inputs) params
    mapM_ (\line -> hPrint out_handle line) calc_out

aux_reader :: [String] -> [(Int, (Int, [Double]))]
aux_reader list_inputs = map make_cluster (concatMap lines list_inputs)
    where
        make_cluster line = read line :: (Int, (Int, [Double]))

main :: IO ()
main = do
    --TODO read from args how many clusters
    --TODO initialize centers randomly (need jobpack utility)
    let centers = [Point 1 [6,1] , Point 2 [1,3]]
    let clusters = 2
    let param = Params centers clusters
    let job = Job {
                map_fun = my_map,
                reduce_fun = my_reducer,
                map_reader = map_reader1,
                reduce_reader = reduce_reader1,
                params = param}
    Worker.run_worker job
    return ()

