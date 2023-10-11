cd ~/miniob
cd src/observer/sql/parser
bash gen_parser.sh
cd ../../../..
bash build.sh clean
bash build.sh --make -j3
