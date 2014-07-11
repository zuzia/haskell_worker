module Task
    where
import System.IO
--TODO
--think about default functions
--class Task where
--map :: Map
--reduce :: Reduce
--init
--done
--params

type Map a = String -> [(Key, a)]
type Reduce a = [(Key, a)] -> [(Key, a)]
type Key = String

--map, reduce gathered type
--TODO
--rewrite to Streams
type Process = [String] -> Handle -> IO ()

