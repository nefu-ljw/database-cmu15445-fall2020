#!/bin/bash

# filename: auto-archive.sh

print_help() {
    echo "Please input correct project number."
    echo ""
    echo "Usage:"
    echo "  $ sh auto-archive.sh \"project number\""
    echo ""
    echo "Example:"
    echo "  $ sh auto-archive.sh 0"
    echo "This will archive project 0."
    echo ""
    echo "Choices:"
    echo "  0 - Project#0 C++ Primer"
    echo "  1 - Project#1 Buffer Pool Manager"
    echo "  2 - Project#2 B+ Tree Index"
    echo "  3 - Project#3 Query Execution"
    echo "  4 - Project#4 Concurrency Control"
}

out="project$1-submission"
if [ -f ./$out.zip ]; then
    rm $out.zip
fi

case $1 in
  4)
    zip $out -urq \
    src/concurrency/lock_manager.cpp \
    src/include/concurrency/lock_manager.h
  ;;
  3)
    zip $out -urq \
    src/include/catalog/catalog.h \
    src/include/execution/execution_engine.h \
    src/include/execution/executor_factory.h \
    src/include/execution/executors/seq_scan_executor.h \
    src/include/execution/executors/index_scan_executor.h \
    src/include/execution/executors/insert_executor.h \
    src/include/execution/executors/update_executor.h \
    src/include/execution/executors/delete_executor.h \
    src/include/execution/executors/nested_loop_join_executor.h \
    src/include/execution/executors/nested_index_join_executor.h \
    src/include/execution/executors/limit_executor.h \
    src/include/execution/executors/aggregation_executor.h \
    src/include/storage/index/b_plus_tree_index.h \
    src/include/storage/index/index.h \
    src/execution/executor_factory.cpp \
    src/execution/seq_scan_executor.cpp \
    src/execution/index_scan_executor.cpp \
    src/execution/insert_executor.cpp \
    src/execution/update_executor.cpp \
    src/execution/delete_executor.cpp \
    src/execution/nested_loop_join_executor.cpp \
    src/execution/nested_index_join_executor.cpp \
    src/execution/limit_executor.cpp \
    src/execution/aggregation_executor.cpp \
    src/storage/index/b_plus_tree_index.cpp
  ;;
  2)
    zip $out -urq \
    src/include/storage/page/b_plus_tree_page.h \
    src/storage/page/b_plus_tree_page.cpp \
    src/include/storage/page/b_plus_tree_internal_page.h \
    src/storage/page/b_plus_tree_internal_page.cpp \
    src/include/storage/page/b_plus_tree_leaf_page.h \
    src/storage/page/b_plus_tree_leaf_page.cpp \
    src/include/storage/index/b_plus_tree.h \
    src/storage/index/b_plus_tree.cpp \
    src/include/storage/index/index_iterator.h \
    src/storage/index/index_iterator.cpp
  ;;
  1)
    zip $out -urq \
    src/include/buffer/lru_replacer.h \
    src/buffer/lru_replacer.cpp \
    src/include/buffer/buffer_pool_manager.h \
    src/buffer/buffer_pool_manager.cpp
  ;;
  0)
    zip $out -urq src/include/primer/p0_starter.h
  ;;
  *)
    # echo "Invalid input: $1\n"
    print_help
    exit 1
  ;;
esac
echo "Files has been writen into $out.zip"
unzip -l $out.zip
