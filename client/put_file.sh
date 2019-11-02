for i in {1..10}
do
    echo "put test$i"
    ./client put_file fa19-cs425-g53-01.cs.illinois.edu :7123 test_file/test$i a/b/c/test$i
    echo ""
done
