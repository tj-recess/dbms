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
    else if(myType == sorted)
        genDBFile = new Sorted();
    else
        return RET_UNSUPPORTED_FILE_TYPE;
    if(!genDBFile)
    {
        cout<<"Not enough memory. EXIT."<<endl;
        exit(1);
    }
    genDBFile->Create(name, startup);
}

// This function assumes that the DBFile already exists
// and has previously been created and then closed.
int DBFile::Open (char *name)
{
    if(genDBFile)
        genDBFile->Open(name);
    else
    {
        //read metadata and create appropriate object
    }
}

// Closes the file.
// The return value is a 1 on success and a zero on failure
int DBFile::Close ()
{
    if(genDBFile)
        return genDBFile->Close();
    else
        return RET_FAILURE;
}

// Bulk loads the DBFile instance from a text file,
// appending new data to it using the SuckNextRecord function from Record.h
// loadMe is the name of the data file to bulk load.
void DBFile::Load (Schema &mySchema, char *loadMe)
{
    if(!genDBFile)
        cout<<"Attempted to Load an unopened file (DEBUG)";
    else
        genDBFile->Load(mySchema, loadMe);
}

// Forces the pointer to correspond to the first record in the file
void DBFile::MoveFirst()
{
    if(!genDBFile)
        cout<<"Attempted to do MoveFirst in an unopened file (DEBUG)";
    else
        genDBFile->MoveFirst();
}

// Add new record to the end of the file
// Note: addMe is consumed by this function and cannot be used again
void DBFile::Add (Record &addMe)
{
    if(!genDBFile)
        cout<<"Attempted to Add in an unopened file (DEBUG)";
    else
        genDBFile->Add(addMe);
}

// Fetch next record (relative to p_currPtr) into fetchMe
int DBFile::GetNext (Record &fetchMe)
{
    if(!genDBFile)
        cout<<"Attempted to Get Next from an unopened file (DEBUG)";
    else
        genDBFile->GetNext(fetchMe);
}

// Applies CNF and then fetches the next record
int DBFile::GetNext (Record &fetchMe, CNF &applyMe, Record &literal)
{
    if(!genDBFile)
        cout<<"Attempted to Get Next from an unopened file (DEBUG)";
//    else
//        genDBFile->
}

// Used in assignment-2
void DBFile::GetPage(Page *putItHere, off_t whichPage)
{

}