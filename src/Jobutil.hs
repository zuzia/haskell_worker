{-# LANGUAGE OverloadedStrings #-}
{-|
Module      : Jobutil
Description : Helper functions for Jobpack module
-}


module Jobutil
    where
import Data.List
import Reader (split_scheme_loc, http_reader)
import Data.Aeson
import Data.Maybe (fromJust)
import Control.Applicative ((<$>), (<*>), empty)
import Control.Monad
import qualified Data.ByteString.Lazy.Char8 as BL

-- | Handle ddfs inputs differently
get_effective_inputs :: [String] -> IO [[String]]
get_effective_inputs inputs =
    mapM ddfs_change inputs

-- | Checks a type of input if it is ddfs tag asks for actual input location
ddfs_change :: String -> IO [String]
ddfs_change inpt =
    let (scheme, rest) = split_scheme_loc inpt in
    if scheme == "tag"
        then do urls <- get_urls rest
                return $ map head urls --TODO only first replica location
        else return [inpt]

-- | Asks Disco for inputs associated with a tag
get_urls :: String -> IO [[String]]
get_urls tag = do
    let url = tag_url tag
    body <- http_reader url
    --check respone status
    let (_,_,urls) = tag_info body
    return urls

data Tag_info = Tag_info {
    version :: Maybe Int,
    tag_id :: Maybe String,
    last_modified :: Maybe String,
    urls :: Maybe [[String]],
    user_data :: Maybe [(String,String)]
} deriving (Show)

--TODO change that instance (problem: last_modified and user_data)
instance FromJSON Tag_info where
    parseJSON (Object v) =
        Tag_info <$>
        (v .:? "version") <*>
        (v .:? "id") <*>
        (v .:? "last_modified") <*>
        (v .:? "urls") <*>
        (v .:? "user_data")
    parseJSON _ = empty


tag_info :: String -> (Int, String, [[String]])
tag_info body = (fromJust $ version t_info,fromJust $ tag_id t_info,fromJust $ urls t_info)
    where
        t_info = fromJust (decode $ BL.pack body :: Maybe Tag_info)

--TODO hardcoded
tag_url :: String -> String
tag_url tag =
    --"http://" ++ Setting("DISCO_MASTER_HOST") ++ ":" ++ Setting("DISCO_PORT") ++ "/ddfs/tag/" ++ tag
    "http://" ++ "localhost" ++ ":" ++ "8989" ++ "/ddfs/tag/" ++ tag


