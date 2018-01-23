cmake_minimum_required(VERSION 2.8.8)

project(btree)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")

set(btree_SOURCES
	memory.c
)

set(testlib_SOURCES
)

set(btree_SOURCE_FILES)
set(btree_HEADER_FILES)
set(testlib_SOURCE_FILES)

foreach(x ${btree_SOURCES})
	list(APPEND btree_SOURCE_FILES "src/${x}")
endforeach()

foreach(x ${testlib_SOURCES})
	list(APPEND testlib_SOURCE_FILES "test/${x}")
endforeach()

include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

#add_executable(alltest test/main.c ${btree_SOURCE_FILES} ${btree_HEADER_FILES})
#set_target_properties(alltest PROPERTIES COMPILE_FLAGS -DTESTING)
#target_link_libraries(alltest -lcunit -ldb)

add_library(btree STATIC ${btree_SOURCE_FILES})
target_link_libraries(btree)
install(TARGETS btree DESTINATION lib)
install(FILES include/btree/memory.h include/btree/common.h DESTINATION include/btree)

