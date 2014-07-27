module Utils
    where
import Task
import Errors
import Control.Exception
import Data.Function
import Data.List
import System.Cmd
import System.Posix.Env
import System.Exit
import System.IO
import System.Directory

kvcompare :: (Key, a) -> (Key, a) -> Bool
kvcompare (k1,_) (k2, _) = k1==k2

kvgroup ::[(Key, a)] -> [[(Key, a)]]
kvgroup l = groupBy kvcompare (sortBy (compare `on` fst) l)

-- it is fold for summing (key,value) lists with the same key in tuple
helper_func :: Num a => a -> [(Key, a)] -> (Key, a)
helper_func acc [(x,y)] = (x,acc+y)
helper_func acc ((x,y):xs) = helper_func (acc+y) xs

sort_cmd :: FilePath -> String -> String
sort_cmd filename sort_buffer_size =
    "sort -k 1,1 -T . -S " ++ sort_buffer_size ++ " -o " ++ filename ++ " " ++ filename

disk_sort :: [String] -> IO String --TODO handle
disk_sort list_inputs = do
    let merged_inputs = concat list_inputs --TODO
    pwd <- getCurrentDirectory
    let filename = pwd ++ "/sorted_inputs"
    handle <- openFile filename ReadWriteMode
    hPutStr handle merged_inputs --TODO large files
    hFlush handle
    hClose handle
    unix_sort filename "10%"
    readFile filename

unix_sort :: FilePath -> String -> IO ()
unix_sort filename sort_buffer_size = do
    setEnv "LC_ALL" "C" True
    let cmd = sort_cmd filename sort_buffer_size
    exit_code <- system cmd
    case exit_code of
        ExitSuccess -> return ()
        ExitFailure num -> throwIO SortingExcept

