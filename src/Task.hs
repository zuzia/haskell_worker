module Task
    where
import System.IO

--started writing the skeleton for a MapReduce job
--need to restructure code a little to handle the readers python style

{--data MapReduce k1 v1 k2 v2 k3 v3 a p m = MapReduce {
    map_fun :: m -> p -> [(k1, v1)],
    reduce_fun :: [(k2, v2)] -> p -> v3,
    map_reader :: [String] -> Handle -> p -> IO (),
    reduce_reader :: [String] -> Handle -> p -> IO (),
    params :: p, --it can be any data type? with haskell there is a problem, because one cannot store values of different types in list, data is more convenient
    combiner :: [(k1, v1)] -> p -> [(k2, v2)], --buffer, done
    map_init :: [String] -> p -> a
--    partition :: k3 -> Int -> p -> Int
--    done
}
--}

data Job k1 v1 k2 v2 v3 m p = Job { 
    map_fun :: Map m p k1 v1,
    reduce_fun :: Reduce k2 v2 p v3,
    map_reader :: Process p,
    reduce_reader :: Process p,
    params :: p
}

type Map m p k1 v1 = m -> p -> [(k1, v1)]
type Reduce k2 v2 p v3 = [(k2, v2)] -> p -> v3
type Key = String

--map, reduce gathered type
--TODO
--rewrite to Streams
type Process p = [String] -> Handle -> p -> IO ()

