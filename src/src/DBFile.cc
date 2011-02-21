#include "DBFile.h"

DBFile::DBFile()
{

}

DBFile::~DBFile()
{

}

// name = location of the file
// fType = heap, sorted, tree
// return value: 1 on success, 0 on failure
int DBFile::Create (char *name, fType myType, void *startup)
{
    if(myType == heap)
        genDBFile = new Heap();
//    else if(myType == sorted)
//        genDBFile = new SortFile();
}

// This function assumes that the DBFile already exists
// and has previously been created and then closed.
int DBFile::Open (char *name)
{

}

// Closes the file.
// The return value is a 1 on success and a zero on failure
int DBFile::Close ()
{

}

// Bulk loads the DBFile instance from a text file,
// appending new data to it using the SuckNextRecord function from Record.h
// loadMe is the name of the data file to bulk load.
void Load (Schema &mySchema, char *loadMe)
{

}

// Forces the pointer to correspond to the first record in the file
void DBFile::MoveFirst()
{

}

// Add new record to the end of the file
// Note: addMe is consumed by this function and cannot be used again
void DBFile::Add (Record &addMe, bool startFromNewPage)
{

}

// Fetch next record (relative to p_currPtr) into fetchMe
int DBFile::GetNext (Record &fetchMe)
{

}

// Applies CNF and then fetches the next record
int DBFile::GetNext (Record &fetchMe, CNF &applyMe, Record &literal)
{

}

// Used in assignment-2
void DBFile::GetPage(Page *putItHere, off_t whichPage)
{

}