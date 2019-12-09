sh join_all.sh
./client put_file fa19-cs425-g53-01.cs.illinois.edu :7123 ../maple/maple maple_exe
./client put_file fa19-cs425-g53-01.cs.illinois.edu :7123 ../juice/juice juice_exe
sh demo_upload.sh
echo "====="
time ./client maple fa19-cs425-g53-01.cs.illinois.edu :7123 maple_exe 7 demo_inter demo/
echo "====="
time ./client juice fa19-cs425-g53-01.cs.illinois.edu :7123 juice_exe 7 demo_inter output_demo.txt 0
