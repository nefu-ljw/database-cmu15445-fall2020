#!/bin/bash

# filename: auto-test.sh

set -e
cd build  # run in {WorkSpace}/build

format=false
while [ $# -gt 0 ]; do
  case $1 in
    -p|--project)
      project=$2
      shift
      shift
      ;;
    -f|--format)
      format=true
      shift
      ;;
  esac
done

run() { 
  echo "\$ $@";  # 打印当前运行的命令
  $@;  # 运行当前命令，@表示所有参数
}

if [ $format = true ]; then
  run make format -j 8
  run make check-lint -j 8
  run make check-clang-tidy -j 8
fi

case $project in
  5)
    run make rollback_test -j 8
    run make grading_transaction_test -j 8
    run make grading_lock_manager_detection_test -j 8
    run make grading_lock_manager_test_1 -j 8
    run make grading_lock_manager_test_2 -j 8
    run make grading_lock_manager_test_3 -j 8
    run ./test/rollback_test
    run ./test/grading_transaction_test
    run ./test/grading_lock_manager_detection_test
    run ./test/grading_lock_manager_test_1
    run ./test/grading_lock_manager_test_2
    run ./test/grading_lock_manager_test_3
  ;;
  4)
    run make grading_catalog_test -j 8
    run make grading_executor_test -j 8
    run ./test/grading_catalog_test
    run ./test/grading_executor_test
  ;;
  3)
    run make grading_b_plus_tree_checkpoint_2_sequential_test -j 8
    run make grading_b_plus_tree_checkpoint_2_concurrent_test -j 8
    run ./test/grading_b_plus_tree_checkpoint_2_sequential_test
    run ./test/grading_b_plus_tree_checkpoint_2_concurrent_test
    run valgrind --trace-children=yes \
              --leak-check=full \
              --show-leak-kinds=all \
              --track-origins=yes \
              "--soname-synonyms=somalloc=*jemalloc*" \
              --error-exitcode=1 \
              --suppressions=../build_support/valgrind.supp \
              ./test/grading_b_plus_tree_checkpoint_2_concurrent_test
  ;;
  2)
    run make grading_b_plus_tree_checkpoint_1_test -j 8
    run ./test/grading_b_plus_tree_checkpoint_1_test
    run valgrind --trace-children=yes \
                 --leak-check=full \
                 --show-leak-kinds=all \
                 --track-origins=yes \
                 "--soname-synonyms=somalloc=*jemalloc*" \
                 --error-exitcode=1 \
                 --suppressions=../build_support/valgrind.supp \
                 ./test/grading_b_plus_tree_checkpoint_1_test
  ;;
  1)  
    run make lru_replacer_test -j 8
    run make grading_buffer_pool_manager_test -j 8
    run make grading_buffer_pool_manager_concurrency_test -j 8
    run make grading_leaderboard_test -j 8
    run ./test/lru_replacer_test
    run ./test/grading_buffer_pool_manager_test
    run ./test/grading_buffer_pool_manager_concurrency_test
    run ./test/grading_leaderboard_test
    run valgrind --trace-children=yes \
              --leak-check=full \
              --show-leak-kinds=all \
              --track-origins=yes \
              "--soname-synonyms=somalloc=*jemalloc*" \
              --error-exitcode=1 \
              --suppressions=../build_support/valgrind.supp \
              ./test/grading_buffer_pool_manager_concurrency_test
  ;;
  0)
    run make starter_test -j 8
    run ./test/starter_test
  ;;
  *)
    echo "Please input correct project number."
    echo ""
    echo "Usage:"
    echo "  sh auto-test [-p proj#] [-f]"
    echo ""
    echo "Description: automate test projects"
    echo "  -f, --format"
    echo "      run formats auto-correction and check before testing"
    echo "  -p proj#, --project proj#"
    echo "      test project proj#. Project numbers:"
    echo "        0 - Project#0 C++ Primer"
    echo "        1 - Project#1 Buffer Pool Manager"
    echo "        2 - Project#2 Checkpoint 1"
    echo "        3 - Project#2 Checkpoint 2"
    echo "        4 - Project#3 Query Execution"
    echo "        5 - Project#4 Concurrency Control"
    exit 1
  ;;
esac
