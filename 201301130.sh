for i in $@
do
	params=" $params $i"
done
g++ -std=c++0x ExternalMergeSort.cpp
./a.out $params
