#include "DBFile.h"
#include <iostream>
#include <stdlib.h>

DBFile::DBfile()
{}

DBFile::~DBFile()
{
	p_currPtr = 0;
}

int DBFile::Create(char *name, fType myType, void *startup)
{
	// check file type
	if (myType != fType.heap)
	{
		cout << "Unsupported file type. Only heap is supported\n";
		return RET_UNSUPPORTED_FILE_TYPE;
	}

	// create a new file. If file with same name already exists,
	// it is wiped clean
	p_currPtr = fopen (name, "w+");	
	if (p_currPtr == NULL)
	{
		cout << "Failed to open file\n";
		return RET_FAILED_FILE_OPEN;
	}
}

int DBFile::Open(char *fname)
{
	// check if file exists
	struct stat fileStat; 
  	int iStatus; 

	iStatus = stat(fname, &fileStat); 
	if (iStatus != 0) 	// file doesn't exists
	{ 
        cout << "File " << fname <<" does not exist.\n";
        return RET_FILE_NOT_FOUND;
	} 

	// TODO: check if file is NOT open already

	// open file in apopend mode, preserving all prev content
	p_currPtr = fopen (name, "a+");                          
    if (p_currPtr == NULL)
    {
        cout << "Failed to open file\n";
        return RET_FAILED_FILE_OPEN;
    }
}

int DBFile::Close()
{
	int ret = fclose(p_currPtr);
	return ret;	
}
