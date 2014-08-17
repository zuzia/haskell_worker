module Protocol_test
    where
import Test.HUnit
import Protocol
import qualified Data.ByteString.Lazy.Char8 as BL

tests = TestList [TestLabel "Parsing input" parse_input_test,
                    TestLabel "All input label" label_all_test]

t_input1 = "[\"done\",[[0,\"ok\",0,[[0,\"disco://localhost/ddfs/vol0/blob/b\"]]]]]"
all_input = "[\"done\",[[0,\"ok\",\"all\",[[0,\"disco://localhost/ddfs/vol0/blob/b\"]]]]]"
bad_input = "[\"done\",[[0,\"ok\",\"bad\",[[0,\"disco://localhost/ddfs/vol0/blob/b\"]]]]]"
t_input5 = "[\"done\",[[0,\"ok\",0,[[0,\"disco://0\"]]],[1,\"ok\",0,[[0,\"disco://1\"]]],[2,\"ok\",0,[[0,\"disco://2\"]]],[3,\"ok\",0,[[0,\"disco://3\"]]],[4,\"ok\",0,[[0,\"disco://4\"]]]]]"

inpt_msg ="[\"done\",[[0,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3107\"]]],[1,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3152\"]]],[2,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3147\"]]],[3,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3142\"]]],[4,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3137\"]]],[5,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3132\"]]],[6,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3127\"]]],[7,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3122\"]]],[8,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3117\"]]],[9,\"ok\",0,[[0,\"disco://localhost/disco/localhost/47/gojob@57d:39dd6:926/map_out_3112\"]]]]]"

parse_input_test = TestCase (do
    p1 <- process_master_msg "INPUT" t_input1
    p5 <- process_master_msg "INPUT" t_input5
    assertEqual "1 input parsing type" (M_task_input (Task_input {input_flag = Done, inputs = [Input {input_id = 0, status = Ok, input_label = Protocol.Label 0, replicas = [Replica {replica_id = 0, replica_location = "disco://localhost/ddfs/vol0/blob/b"}]}]})) p1
    let M_task_input t1 = p1
    let [rep1] = (replicas . head . inputs) t1
    let rep_loc1 = replica_location rep1 
    assertEqual "1 input parsing replica location" "disco://localhost/ddfs/vol0/blob/b" rep_loc1
    let M_task_input t5 = p5
    let len5 = length $ inputs t5
    assertEqual "5 input parsing" 5 len5
    M_task_input t_mult <- process_master_msg "INPUT" inpt_msg
    let len_mult = length $ inputs t_mult
    assertEqual "Multiple input parsing" 10 len_mult)

label_all_test = TestCase (do
    M_task_input all <- process_master_msg "INPUT" all_input
    let inpt = (head . inputs) all
    let bad =  process_master_msg "INPUT" bad_input :: Maybe Master_msg
    assertEqual "All input label" All (input_label inpt)
    assertEqual "Bad input label" Nothing bad)
 
main = runTestTT tests

