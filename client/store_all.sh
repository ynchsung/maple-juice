for i in {1..10}
do
    echo "machine $i"
    ./client store fa19-cs425-g53-01.cs.illinois.edu :7123 $i
    echo ""
done
