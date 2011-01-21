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

enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};


unsigned int Random_Generate();


#endif

