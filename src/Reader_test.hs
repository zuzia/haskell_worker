module Reader_test
    where

import Test.HUnit
import Reader
import Protocol

tests = TestList [TestLabel "scheme test" scheme_test,
            TestLabel "convert uri" convert_uri_test,
            TestLabel "abs paths" abs_paths_test]

local = "localhost/disco/localhost/fa/gojob@57d:42bbf:148ed/map_out_3022"

not_local = "notlocalhost/disco/localhost/fa/gojob@57d:42bbf:148ed/map_out_3022"

task = Task {taskid = 11, master = "http://monadek.localdomain:8989", disco_port = 8989, put_port = 8990, ddfs_data = "/usr/local/var/disco/ddfs", disco_data = "/usr/local/var/disco/data", stage = "reduce", grouping = "group_all", group = (0,""), jobfile = "/usr/local/var/disco/data/localhost/47/gojob@57d:39dd6:926/jobfile", jobname = "gojob@57d:39dd6:926", host = "localhost"}

scheme_test = TestCase (do
    let disco = get_scheme "disco://haskell"
    let raw = get_scheme "raw://haskell"
    let dir = get_scheme "dir://haskell"
    let http = get_scheme "http://haskell"
    let https = get_scheme "https://haskell"
    assertEqual "disco scheme" (SDisco, "haskell") disco
    assertEqual "raw scheme" (SRaw, "haskell") raw
    assertEqual "dir scheme" (SDir, "haskell") dir
    assertEqual "http scheme" (SHttp, "http://haskell") http
    assertEqual "https scheme" (SHttp, "https://haskell") https)

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
    assertEqual "Absolute disco path" "/usr/local/var/disco/data/localhost/fa/gojob@57d:42bbf:148ed/map_out_3022" p_disco
    assertEqual "Absolute ddfs path" "/usr/local/var/disco/ddfs/vol0/blob/3c/xab$57b-404a5-57476" p_ddfs)

-- TEST CASES
-- simple:
-- * read_inputs
-- * address_reader
-- * read_dir_rest
-- * dir_reader
-- * disco_reader 
-- * http_reader
-- * get_data_type
-- * disco_output_path

