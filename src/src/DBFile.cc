#include "DBFile.h"

DBFile::DBFile()
{
	m_pFile = new File();
}

DBFile::~DBFile()
{
	// set FILE handle to null
	m_pCurrPtr = 0;

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

int DBFile::Create(char *f_path, fType f_type, void *startup)
{
	// check file type
	if (f_type != heap)
	{
		cout << "Unsupported file type. Only heap is supported\n";
		return RET_UNSUPPORTED_FILE_TYPE;
	}

	// open a new file. If file with same name already exists
	// it is wiped clean
	int iOpenExistingFile = 0;
	if (m_pFile)
		m_pFile->Open(iOpenExistingFile, f_path);

	//TODO: close file here?
	int ret = Close();
	return ret;
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

	// open file in append mode, preserving all prev content
	int iOpenExistingFile = 1;
	if (m_pFile)
		m_pFile->Open(iOpenExistingFile, fname);

	// TODO: error checking if open failed?
	
	return RET_SUCCESS;
}

int DBFile::Close()
{
	//TODO
	return 0;
}

void DBFile::Load (Schema &mySchema, char *loadMe)
{
	m_pCurrPtr = fopen (loadMe, "r");
	if (!m_pCurrPtr)
	{
		// file to load could not be opened
		// print error
	}

	// Pass the schema and file ptr to SuckNextRecord
	// set m_pRecord, if not alreadt set....?
	if (!m_pRecord)
		m_pRecord = new Record();

	if (m_pRecord)
		m_pRecord->SuckNextRecord(&mySchema, m_pCurrPtr);
	else
	{
		// fatal error
		// m_pRecord should not be NULL at this point
	}

	// bits have been populated
	// use m_pRecord->GetBits() and store data in page/file

	delete m_pRecord;
	m_pRecord = NULL;
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

