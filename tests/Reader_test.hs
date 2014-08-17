module Reader_test
    where

import Test.HUnit
import Reader
import Protocol
import Errors
import Control.Exception
import System.IO.Temp (withSystemTempDirectory, withSystemTempFile)
import System.IO

--TODO read_dir_rest

tests = TestList [TestLabel "Scheme test" scheme_test,
            TestLabel "Convert uri" convert_uri_test,
            TestLabel "Abs paths" abs_paths_test,
            TestLabel "Read dir inputs test" read_dir_inputs_test,
            TestLabel "Dir test with dir file" dir_test,
            TestLabel "Addr read" address_reader_test]

local = "localhost/disco/localhost/fa/gojob@57d:42bbf:148ed/map_out_3022"
dir_local = "dir://localhost/disco/localhost/"

not_local = "notlocalhost/disco/localhost/fa/gojob@57d:42bbf:148ed/map_out_3022"

task = Task {taskid = 11, master = "http://monadek.localdomain:8989", disco_port = 8989, put_port = 8990, ddfs_data = "/usr/local/var/disco/ddfs", disco_data = "/usr/local/var/disco/data", stage = "reduce", grouping = "group_all", group = (0,""), jobfile = "/usr/local/var/disco/data/localhost/47/gojob@57d:39dd6:926/jobfile", jobname = "gojob@57d:39dd6:926", host = "localhost"}

scheme_test = TestCase (do
    let disco = get_scheme "disco://haskell"
    let raw = get_scheme "raw://haskell"
    let dir = get_scheme "dir://haskell"
    let http = get_scheme "http://haskell"
    let https = get_scheme "https://haskell"
    let bad = get_scheme "not://haskell"
    assertEqual "Disco scheme" (Just (SDisco, "haskell")) disco
    assertEqual "Raw scheme" (Just (SRaw, "haskell")) raw
    assertEqual "Dir scheme" (Just (SDir, "haskell")) dir
    assertEqual "Http scheme" (Just (SHttp, "http://haskell")) http
    assertEqual "Https scheme" (Just (SHttp, "https://haskell")) https
    assertEqual "Bad scheme" Nothing bad)

convert_uri_test = TestCase (do
    let (schem1, addr1) = convert_uri SDisco local task
    assertEqual "Convert uri local disco scheme" SDisco schem1
    assertEqual "Convert uri local disco addr" local addr1
    let (schem2, addr2) = convert_uri SDisco not_local task
    assertEqual "Convert uri not local disco scheme" SHttp schem2
    assertEqual "Convert uri not local disco addr" "http://notlocalhost:8989/disco/localhost/fa/gojob@57d:42bbf:148ed/map_out_3022" addr2
    assertEqual "Convert uri not changed, http scheme" (SHttp, "http://haskell") (convert_uri SHttp "http://haskell" task))

abs_paths_test = TestCase (do
    let p_disco = absolute_disco_path "localhost/disco/localhost/fa/gojob@57d:42bbf:148ed/map_out_3022" task
    let p_ddfs = absolute_ddfs_path "localhost/ddfs/vol0/blob/3c/xab$57b-404a5-57476" task
    bad_disco <- try (evaluate $ absolute_disco_path "localhost/bad/localhost/fa/" task) :: IO (Either WorkerException String)
    bad_ddfs <- try (evaluate $ absolute_ddfs_path "localhost/bad/vol0/" task) :: IO (Either WorkerException String)
    assertEqual "Absolute disco path" "/usr/local/var/disco/data/localhost/fa/gojob@57d:42bbf:148ed/map_out_3022" p_disco
    assertEqual "Absolute ddfs path" "/usr/local/var/disco/ddfs/vol0/blob/3c/xab$57b-404a5-57476" p_ddfs
    assertEqual "Bad absolute disco path" (Left AbsPathExcept) bad_disco
    assertEqual "Bad absolute ddfs path" (Left AbsPathExcept) bad_ddfs)

set_up_tmp tmp_handle str = do
    hPutStrLn tmp_handle str
    hClose tmp_handle

addr_read_helper scheme_str tmp_file tmp_handle = do
    let task1 = task {disco_data = ""}
    set_up_tmp tmp_handle "haskell fun"
    Right contents <- try (address_reader (scheme_str ++ tmp_file) task1) :: IO (Either SomeException String)
    assertEqual ("Address reader test " ++ scheme_str) contents "haskell fun\n"

address_reader_test = TestCase (do
    Right contents <- try (address_reader "http://www.haskell.org" task) :: IO (Either SomeException String)
    assertBool "Simple http test" (""/=contents)
    withSystemTempFile "disco_temp_" (\tmp_f tmp_h -> addr_read_helper "disco://localhost/disco" tmp_f tmp_h))

dir_action1 temp_dir = do
    (tempFilePath, tempHandle) <- openTempFile temp_dir "temp_file_"
    let task1 = task {disco_data = ""}
    hPutStrLn tempHandle "0 http://www.haskell.org 300"
    hPutStrLn tempHandle "1 http://google.com 1000"
    hClose tempHandle
    try (read_dir_rest ("dir://localhost/disco" ++ tempFilePath) task1) :: IO (Either SomeException [String])

dir_action2 temp_dir = do
    (tempFilePath, tempHandle) <- openTempFile temp_dir "temp_file_"
    (tempDisco1, tempHandleDisco1) <- openTempFile temp_dir "disco_file_"
    (tempDisco2, tempHandleDisco2) <- openTempFile temp_dir "disco_file_"
    let task1 = task {disco_data = ""}
    hPutStrLn tempHandle $ "0 disco://localhost/disco" ++ tempDisco1 ++ " 300"
    hPutStrLn tempHandle $ "1 disco://localhost/disco" ++ tempDisco2 ++ " 300"
    hClose tempHandle
    set_up_tmp tempHandleDisco1 "haskell fun"
    set_up_tmp tempHandleDisco2 "even more fun"
    try (read_dir_rest ("dir://localhost/disco" ++ tempFilePath) task1) :: IO (Either SomeException [String])

dir_test = TestCase (do
    Right contents1 <- withSystemTempDirectory "test_dir_" dir_action1
    assertBool "Read dir with http lines" (2 == length contents1)
    Right contents2 <- withSystemTempDirectory "test_dir_" dir_action2
    assertBool "Read dir action 2" (2 == length contents2)
    assertEqual "Read dir action 2 disco 1" (head contents2) "haskell fun\n"
    assertEqual "Read dir action 2 disco 2" (last contents2) "even more fun\n")

read_dir_inputs_test = TestCase (do
    res1 <- try (read_dir_rest "sth" task) :: IO (Either WorkerException [String])
    assertEqual "Reading Dir, unknown scheme" (Left UnknownScheme) res1
    res2 <- try (read_dir_rest "dir://sth" task) :: IO (Either WorkerException [String])
    assertBool "Reading Dir, http error" (res2 == Left (HttpExcept "user error (openTCPConnection: host lookup failure for \"sth\")")))

main = do
    runTestTT tests

