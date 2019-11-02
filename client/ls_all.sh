for i in {1..10}
do
    echo "test$i"
    ./client ls fa19-cs425-g53-01.cs.illinois.edu :7123 a/b/c/test$i
    echo ""
done
