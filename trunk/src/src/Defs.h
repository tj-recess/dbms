#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072

// Error codes
#define RET_FAILURE 0
#define RET_SUCCESS 1
#define RET_FILE_NOT_FOUND 2
#define RET_FAILED_FILE_OPEN 3
#define RET_UNSUPPORTED_FILE_TYPE 4
#define RET_READING_PAST_EOF 0
#define RET_FETCHING_REC_FAIL 0

enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};

// Enum for file opening modes
enum FileOpenMode { TRUNCATE = 0, APPEND = 1};


unsigned int Random_Generate();

//#define _DEBUG 0
//#define _Sorted_DEBUG 1
//#define _OPS_DEBUG

#endif

