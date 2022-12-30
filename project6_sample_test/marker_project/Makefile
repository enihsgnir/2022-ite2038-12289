CXX=g++
#CXXFLAGS= -g -std=c++17 -O2 -Wall

CXXFLAGS= -g -Wall
LDFLAGS=-L./ -I./ -ldb -lpthread

OBJS=marker.o
TARGET=marker


all: clean $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $^ -c -o $@ $(LDFLAGS)



INT=10000

test01: clean all
	$(RM) DATA? *.txt LOG LOGMSG
	./$(TARGET) logging logging_simple_test 00_output.txt 00_result.txt 00_committed_trx.txt non_deterministic_mode population    before_crash $(INT)
	cp DATA1 ../populated_db/logging_simple_test_DATA1
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_simple_test 01_output.txt 01_result.txt 01_committed_trx.txt non_deterministic_mode compatibility before_crash $(INT)
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_simple_test 02_output.txt 02_result.txt 02_committed_trx.txt non_deterministic_mode no_insertion  before_crash $(INT)
	./$(TARGET) logging logging_simple_test 03_output.txt 03_result.txt 03_db_state.txt      non_deterministic_mode no_insertion  normal_recovery $(INT)
	cat *_result.txt | grep "INCORRECT" || true

test02: clean all
	$(RM) DATA? *.txt LOG LOGMSG
	./$(TARGET) logging logging_short_transaction_nodeadlock_test 00_output.txt 00_result.txt 00_committed_trx.txt non_deterministic_mode population    before_crash $(INT)
	cp DATA1 ../populated_db/logging_short_transaction_nodeadlock_test_DATA1
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_short_transaction_nodeadlock_test 01_output.txt 01_result.txt 01_committed_trx.txt non_deterministic_mode compatibility before_crash $(INT)
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_short_transaction_nodeadlock_test 02_output.txt 02_result.txt 02_committed_trx.txt non_deterministic_mode no_insertion  before_crash $(INT)
	./$(TARGET) logging logging_short_transaction_nodeadlock_test 03_output.txt 03_result.txt 03_db_state.txt      non_deterministic_mode no_insertion  normal_recovery $(INT)
	cat *_result.txt | grep "INCORRECT" || true

test03: clean all
	$(RM) DATA? *.txt LOG LOGMSG
	./$(TARGET) logging logging_short_transaction_test 00_output.txt 00_result.txt 00_committed_trx.txt non_deterministic_mode population    before_crash $(INT)
	cp DATA1 ../populated_db/logging_short_transaction_test_DATA1
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_short_transaction_test 01_output.txt 01_result.txt 01_committed_trx.txt non_deterministic_mode compatibility before_crash $(INT)
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_short_transaction_test 02_output.txt 02_result.txt 02_committed_trx.txt non_deterministic_mode no_insertion  before_crash $(INT)
	./$(TARGET) logging logging_short_transaction_test 03_output.txt 03_result.txt 03_db_state.txt      non_deterministic_mode no_insertion  normal_recovery $(INT)
	cat *_result.txt | grep "INCORRECT" || true

test04: clean all
	$(RM) DATA? *.txt LOG LOGMSG
	./$(TARGET) logging logging_long_transaction_nodeadlock_test 00_output.txt 00_result.txt 00_committed_trx.txt non_deterministic_mode population    before_crash $(INT)
	cp DATA1 ../populated_db/logging_long_transaction_nodeadlock_test_DATA1
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_long_transaction_nodeadlock_test 01_output.txt 01_result.txt 01_committed_trx.txt non_deterministic_mode compatibility before_crash $(INT)
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_long_transaction_nodeadlock_test 02_output.txt 02_result.txt 02_committed_trx.txt non_deterministic_mode no_insertion  before_crash $(INT)
	./$(TARGET) logging logging_long_transaction_nodeadlock_test 03_output.txt 03_result.txt 03_db_state.txt      non_deterministic_mode no_insertion  normal_recovery $(INT)
	cat *_result.txt | grep "INCORRECT" || true

test05: clean all
	$(RM) DATA? *.txt LOG LOGMSG
	./$(TARGET) logging logging_long_transaction_test 00_output.txt 00_result.txt 00_committed_trx.txt non_deterministic_mode population    before_crash $(INT)
	cp DATA1 ../populated_db/logging_long_transaction_test_DATA1
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_long_transaction_test 01_output.txt 01_result.txt 01_committed_trx.txt non_deterministic_mode compatibility before_crash $(INT)
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_long_transaction_test 02_output.txt 02_result.txt 02_committed_trx.txt non_deterministic_mode no_insertion  before_crash $(INT)
	./$(TARGET) logging logging_long_transaction_test 03_output.txt 03_result.txt 03_db_state.txt      non_deterministic_mode no_insertion  normal_recovery $(INT)
	cat *_result.txt | grep "INCORRECT" || true

test06: clean all
	$(RM) DATA? *.txt LOG LOGMSG
	./$(TARGET) logging logging_live_transaction_nodeadlock_test 00_output.txt 00_result.txt 00_committed_trx.txt non_deterministic_mode population    before_crash $(INT)
	cp DATA1 ../populated_db/logging_live_transaction_nodeadlock_test_DATA1
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_live_transaction_nodeadlock_test 01_output.txt 01_result.txt 01_committed_trx.txt non_deterministic_mode compatibility before_crash $(INT)
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_live_transaction_nodeadlock_test 02_output.txt 02_result.txt 02_committed_trx.txt non_deterministic_mode no_insertion  before_crash $(INT)
	./$(TARGET) logging logging_live_transaction_nodeadlock_test 03_output.txt 03_result.txt 03_db_state.txt      non_deterministic_mode no_insertion  normal_recovery $(INT)
	cat *_result.txt | grep "INCORRECT" || true

test07: clean all
	$(RM) DATA? *.txt LOG LOGMSG
	./$(TARGET) logging logging_live_transaction_test 00_output.txt 00_result.txt 00_committed_trx.txt non_deterministic_mode population    before_crash $(INT)
	cp DATA1 ../populated_db/logging_live_transaction_test_DATA1
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_live_transaction_test 01_output.txt 01_result.txt 01_committed_trx.txt non_deterministic_mode compatibility before_crash $(INT)
	$(RM) LOG LOGMSG
	./$(TARGET) logging logging_live_transaction_test 02_output.txt 02_result.txt 02_committed_trx.txt non_deterministic_mode no_insertion  before_crash $(INT)
	./$(TARGET) logging logging_live_transaction_test 03_output.txt 03_result.txt 03_db_state.txt      non_deterministic_mode no_insertion  normal_recovery $(INT)
	cat *_result.txt | grep "INCORRECT" || true

#gdb -ex=run --args ./$(TARGET) logging logging_long_transaction_test 02_output.txt 02_result.txt 02_committed_trx.txt non_deterministic_mode no_insertion  before_crash $(INT)
#gdb -ex=run --args ./$(TARGET) logging logging_short_transaction_test 03_output.txt 03_result.txt 03_db_state.txt      non_deterministic_mode no_insertion  normal_recovery $(INT)



testall: test01 test02 test03 test04 test05 test06 test07

clean:
	$(RM) *.o $(TARGET)
	$(RM) *.txt
	$(RM) DATA?
	$(RM) LOG
	$(RM) LOGMSG
