#include "DBFile.h"

DBFile::DBFile(): m_sFilePath(), m_pPage(NULL), m_nTotalPages(0),
				  m_bDirtyPageExists(false), m_bIsDirtyMetadata(false),
				  m_pCurrPage(NULL), m_nCurrPage(0), m_nCurrRecord(0), 
				  m_pCurrPageCNF(NULL), m_nCurrPageCNF(0), m_nCurrRecordCNF(0)
{
	m_pFile = new File();
}

DBFile::~DBFile()
{
	// delete member File pointer
	if (m_pFile)
	{
		delete m_pFile;
		m_pFile = NULL;
	}

	// delete member Page pointer
	if (m_pPage)
	{
		delete m_pPage;
		m_pPage = NULL;
	}

	// delete current page pointer
	if (m_pCurrPage)
	{
		delete m_pCurrPage;
		m_pCurrPage = NULL;
	}

	// delete current page CNF pointer
	if (m_pCurrPageCNF)
	{
		delete m_pCurrPageCNF;
		m_pCurrPageCNF = NULL;
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

	// saving file path (name)
	m_sFilePath = f_path;

	// open a new file. If file with same name already exists
	// it is wiped clean
	if (m_pFile)
		m_pFile->Open(TRUNCATE, f_path);

	//TODO: close file here?
	return Close();
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

	// TODO: check if file is NOT open already - why ?
	// ans - coz we should not access an already open file
	// eg - currently open by another thread (?)

	// Read m_nTotalPages from metadata file, if meta.data file exists
	string meta_file_name = m_sFilePath + ".meta.data";
	iStatus = stat(meta_file_name.c_str(), &fileStat);
	if (iStatus == 0)
	{
		ifstream meta_in;
		meta_in.open(meta_file_name.c_str(), ifstream::in);
		meta_in >> m_nTotalPages;
		meta_in.close();
	}

	// open file in append mode, preserving all prev content
	if (m_pFile)
	{
		m_pFile->Open(APPEND, const_cast<char*>(fname));//openInAppendMode
	}
	// TODO: error checking if open failed?
	
	return RET_SUCCESS;
}

// returns 1 if successfully closed the file, 0 otherwise 
int DBFile::Close()
{
	//TODO : how to find if m_pFile->Close failed ?
	m_pFile->Close();
	
	// write total pages to <table_name>.meta.data
	WriteMetaData();

	return 1; // If control came here, return success
}

/* Load function bulk loads the DBFile instance from a text file, appending
 * new data to it using the SuckNextRecord function from Record.h. The character
 * string passed to Load is the name of the data file to bulk load.
 */
void DBFile::Load (Schema &mySchema, char *loadMe)
{
	EventLogger *el = EventLogger::getEventLogger();

	FILE *fileToLoad = fopen(loadMe, "r");
	if (!fileToLoad)
	{
		el->writeLog("Can't open file name :" + string(loadMe));
	}

	//open the dbfile instance
	Open(const_cast<char*>(m_sFilePath.c_str()));

	/* Logic : 
     * first read the record from the file using suckNextRecord()
	 * then add this record to page using Add() function
	 * Write dirty data to file before leaving this function
	 */

	Record aRecord;
	while(aRecord.SuckNextRecord(&mySchema, fileToLoad))
		Add(aRecord);

	WritePageToFile();
}

void DBFile::Add (Record &rec)
{
	Record aRecord;
	aRecord.Consume(&rec);

	/* Logic: 
	 * Try adding the record to the current page
	 * if adding fails, write page to file and create new page
     * mark m_bDirtyPageExists = true after adding record to page
	 */

	// Writing data in the file for the first time
	if (m_pPage == NULL)
	{
		m_pPage = new Page();
		m_nTotalPages = 0;
	}

	// a page exists in memory, add record to it
	if (m_pPage)	
	{
	    if (!m_pPage->Append(&aRecord)) // current page does not have enough space
    	{
        	// write current page to file
	        // this function will fetch a new page too
    	    WritePageToFile();
        	if (!m_pPage->Append(&aRecord))
	        {
				cout << "DBFile::Add --> Adding record to page failed.\n";
				return;
        	}
			else
				m_bDirtyPageExists = true;
	    }
		else
			m_bDirtyPageExists = true;
	}
}

void DBFile::MoveFirst ()
{
	// Reset current page and record pointers
	m_nCurrPage = 0; 
	m_nCurrRecord = 0;
	delete m_pCurrPage;
	m_pCurrPage = NULL;

	m_nCurrPageCNF = 0; 
	m_nCurrRecordCNF = 0;
	delete m_pCurrPageCNF;
	m_pCurrPageCNF = NULL;
	
	// Malvika's note: This function can be optimized
	// Don't delete m_pCurrPage(CNF) if it is the first page itself
}

// Function to fetch the next record in the file in "fetchme"
// Returns 0 on failure
int DBFile::GetNext (Record &fetchme)
{
	return FetchNextRec(fetchme, &m_pCurrPage, m_nCurrPage, m_nCurrRecord);
}


// Function to fetch the next record in "fetchme" that matches 
// the given CNF, returns 0 on failure.
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
	/* logic :
	 * first read the record from the file in "fetchme,
	 * pass it for comparison using given cnf and literal.
	 * if compEngine.compare returns 1, it means fetched record
	 * satisfies CNF expression so we simple return success (=1) here
	 */

	ComparisonEngine compEngine;

	// FetchNextRec(Record&) handles dirty pages possibility
	while (FetchNextRec(fetchme, &m_pCurrPageCNF, 
					   m_nCurrPageCNF, m_nCurrRecordCNF)) 
	{
		if (compEngine.Compare(&fetchme, &literal, &cnf))
			return RET_SUCCESS;
	}

	//if control is here then no matching record was found
	return RET_FAILURE;
}


/* Private function that fetched the next record in "fetchme"
 * from the page "pCurrPage". It also updates the variables 
 * nCurrPage and nCurrRecord which are passed to it by reference
 * Returns 0 on failure
 * This function is called by the following two functions:
 * int DBFile::GetNext (Record &fetchme) and
 * int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal)
 */
int DBFile::FetchNextRec (Record &fetchme, Page ** pCurrPage, 
						  int &nCurrPage, int &nCurrRecord)
{
	// write dirty page to file
	// as it might happen that the record we want to fetch now
	// is still in the dirty page which has not been flushed to disk
	WritePageToFile();

	// coming for the first time
	// Page starts with 0, but data is stored from 1st page onwards
	// Refer to File :: GetPage (File.cc line 168)
	if (nCurrPage == 0)
	{
		// Store a copy of the page in member buffer
		*pCurrPage = new Page();
		(*pCurrPage)->EmptyItOut();
		m_pFile->GetPage(*pCurrPage, nCurrPage++);
	}

	if (*pCurrPage)
	{
		// Try to fetch the first record from current_page
		// This function will delete this record from the page
		int ret = (*pCurrPage)->GetFirst(&fetchme);
		if (!ret)
		{
			// Check if pages are still left in the file
			// Note: first page in File doesn't store the data
			// So if GetLength() returns 2 pages, data is actually stored in only one page
			if (nCurrPage < m_pFile->GetLength()-1)	
			{											
				// page ran out of records, so empty it and fetch next page
				(*pCurrPage)->EmptyItOut();
				m_pFile->GetPage(*pCurrPage, nCurrPage++);
				ret = (*pCurrPage)->GetFirst(&fetchme);
				if (!ret) // failed to fetch next record
				{
					// check if we have reached the end of file
					if (nCurrPage >= m_pFile->GetLength())
					{
						cout << "DBFile::GetNext --> End of file reached."
							 << "Error trying to fetch more records\n";
						return RET_FAILURE;
					}
					else
					{
						cout << "DBFile::GetNext --> End of file not reached, "
							 << "but fetching record from file failed!\n";
						return RET_FAILURE;
					}
				}
			}
			else	// end of file reached, cannot read more
				return RET_FAILURE;
		}
		// Record fetched successfully
		nCurrRecord++;
		return RET_SUCCESS;
	}
	else
	{
		cout << "DBFile::FetchNextRec --> pCurrPage is NULL. "
			 << "Fatal error!\n";
		return RET_FAILURE;
	}

	return RET_SUCCESS;
}

// Write dirty page to file
void DBFile::WritePageToFile()
{
	if (m_bDirtyPageExists)
	{
		m_pFile->AddPage(m_pPage, m_nTotalPages++);
		m_pPage->EmptyItOut();
		// everytime page count increases, set m_bIsDirtyMetadata to true
		m_bIsDirtyMetadata = true;
	}
    m_bDirtyPageExists = false;
}

// Create <table_name>.meta.data file
// And write total pages used for table loading in it
void DBFile::WriteMetaData()
{
   if (m_bIsDirtyMetadata && !m_sFilePath.empty())
   {
		ofstream meta_out;
		meta_out.open(string(m_sFilePath + ".meta.data").c_str(), ios::trunc);
		meta_out << m_nTotalPages;
		meta_out.close();
		m_bIsDirtyMetadata = false;
   }
}
