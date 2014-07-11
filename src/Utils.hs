module Utils
    where
import Task
import Data.Function
import Data.List

kvcompare :: (Key, a) -> (Key, a) -> Bool
kvcompare (k1,_) (k2, _) = k1==k2

kvgroup ::[(Key, a)] -> [[(Key, a)]]
kvgroup l = groupBy kvcompare (sortBy (compare `on` fst) l)

-- it is fold for summing (key,value) lists with the same key in tuple
helper_func :: Num a => a -> [(Key, a)] -> (Key, a)
helper_func acc [(x,y)] = (x,acc+y)
helper_func acc ((x,y):xs) = helper_func (acc+y) xs

