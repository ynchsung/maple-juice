sh join_all.sh
./client put_file fa19-cs425-g53-01.cs.illinois.edu :7123 ../maple2/maple2 maple2_exe
./client put_file fa19-cs425-g53-01.cs.illinois.edu :7123 ../juice2/juice2 juice2_exe
sh graph_upload.sh
echo "====="
time ./client maple fa19-cs425-g53-01.cs.illinois.edu :7123 maple2_exe 9 graph_inter graph/
echo "====="
time ./client juice fa19-cs425-g53-01.cs.illinois.edu :7123 juice2_exe 9 graph_inter output_graph.txt 0
