cd ./project6/db_project

cmake -S . -B build
cmake --build build

./build/bin/db_test

cp ./build/lib/libdb.a ../../project6_sample_test/marker_project
rm -rf build

cd ../../project6_sample_test
mkdir populated_db

cd ./marker_project
make testall
make clean
rm libdb.a

cd ..
rm -rf populated_db

cd ..
