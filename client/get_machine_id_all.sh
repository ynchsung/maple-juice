for i in $(seq -f "%02g" 1 10)
do
    echo "machine $i"
    ./client get_machine_id fa19-cs425-g53-$i.cs.illinois.edu :7123
    echo ""
done
