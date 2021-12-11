#!/bin/bash
zip -r project1.zip \
	src/include/buffer/lru_replacer.h \
	src/buffer/lru_replacer.cpp \
	src/include/buffer/buffer_pool_manager.h \
	src/buffer/buffer_pool_manager.cpp
