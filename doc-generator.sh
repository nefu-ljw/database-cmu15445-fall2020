#!/bin/bash

rm -rf ./doc
mkdir doc
cd doc

cp -r ../src ./
doxygen -g Doxygen.config  # 生成配置文件

# 修改配置文件 sed -i
sed -i '867c RECURSIVE              = YES' Doxygen.config
sed -i '35c PROJECT_NAME           = "CMU Project"' Doxygen.config

doxygen Doxygen.config     # 根据代码生成文档

rm -rf ./src

# 打包
zip -r doc.zip ./

