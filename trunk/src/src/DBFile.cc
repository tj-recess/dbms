#include "DBFile.h"
#include <iostream>
#include <stdlib.h>

DBFile::DBfile()
{
	m_pFile = new File();
}

DBFile::~DBFile()
{
	// set FILE handle to null
	p_currPtr = 0;

	// delete member File pointer
	if (m_pFile)
	{
		delete m_pFile;
		m_pFile = NULL;
	}

	// delete memeber Record pointer
	if (m_pRecord)
	{
		delete m_pRecord;
		m_pRecord = NULL;
	}
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
	//close file
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
	// TODO: use File::Open instead of opening it here
	// int iOpenExistingFile = 1;
	// m_pFile->Open(iOpenExistingFile, fname);
	// but it will lseek using the 1st parameter... is that okay?

	p_currPtr = fopen (fname, "a+");                          
    if (p_currPtr == NULL)
    {
        cout << "Failed to open file\n";
        return RET_FAILED_FILE_OPEN;
    }
	
	return RET_SUCCESS;
}

int DBFile::Close()
{
	int ret = fclose(p_currPtr);
	return ret;	
}

void DBFile::Load (Schema &mySchema, char *loadMe)
{
	// Open the loadMe file, p_currPtr will be set
	int ret = open(loadMe);	
	if (ret != RET_SUCCESS)
	{
		// file to load could not be opened
		// print error
	}

	// Pass the schema and file ptr to SuckNextRecord
	// set m_pRecord, if not alreadt set....?
	if (!m_pRecord)
		m_pRecord = new Record();

	if (m_pRecord)
		m_pRecord->SuckNextRecord(mySchema, p_currPtr);
	else
	{
		// fatal error
		// m_pRecord should not be NULL at this point
	}

	// bits have been populated
	// use m_pRecord->GetBits() and store data in page/file
}



/*
 * #include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
}

int DBFile::Open (char *f_path) {
}

void DBFile::MoveFirst () {
}

int DBFile::Close () {
}

void DBFile::Add (Record &rec) {
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
 */
 */
