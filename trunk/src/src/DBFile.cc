#include "DBFile.h"

DBFile::DBFile()
{
	m_pFile = new File();
	m_pPage = new Page();
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
}

int DBFile::Create(char *f_path, fType f_type, void *startup)
{
	// check file type
	if (f_type != heap)
	{
		cout << "Unsupported file type. Only heap is supported\n";
		return RET_UNSUPPORTED_FILE_TYPE;
	}
	//saving file path (name)
	m_sFilePath = f_path;
	// open a new file. If file with same name already exists
	// it is wiped clean

	if (m_pFile)
		m_pFile->Open(openInTruncateMode, f_path);

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
	if (m_pFile)
		m_pFile->Open(openInAppendMode, const_cast<char*>(fname));

	// TODO: error checking if open failed?
	
	return RET_SUCCESS;
}

/*
 * returns 1 if successfully closed the file,
 * 0 otherwise
 */
int DBFile::Close()
{
	//TODO : how to find if m_pFile->Close failed ?
	m_pFile->Close();
	return 1;//if control is here, always return success
}

/*
 * Load function bulk loads the DBFile instance from a text file, appending
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
	Open((char*)m_sFilePath.c_str());

	/*
	 * logic : first read the record from the file using suckNextRecord()
	 * then add this record to page and keep adding such records until page is full
	 * once page is full, write it to file.
	 */

	Record aRecord;
	el->writeLog("Can't create a record, not enough memory!");

	while(aRecord.SuckNextRecord(&mySchema, fileToLoad))
		if(!m_pPage->Append(&aRecord))
		{
			WritePageToFile();
		}
}

void DBFile::Add (Record &rec)
{
	Record aRecord;
	aRecord.Consume(&rec);

	// TODO: fetch last used page into m_pPage

	// Try to store the record into current page
	int ret = m_pPage->Append(&aRecord);
	if (!ret)	// current page does not have enough space
	{
		// write current page to file
		// this function will create a new page too
		WritePageToFile();
		ret = m_pPage->Append(&aRecord);
		if (!ret)
		{
			// error logger
		}
	}
}

void DBFile::MoveFirst ()
{
	WritePageToFile(); // --> need to write the dirty page?
}

int DBFile::GetNext (Record &fetchme)
{
	// write dirty page to file
	// as it might happen that the record we want to fetch now
	// is still in the dirty page which has not been flushed to disk
	WritePageToFile();

	// coming for the first time
	// TODO: page stars from 0 or 1?
	if (m_nCurrPage == 0)
	{
		// Store a copy of the page in member buffer
		m_pCurrPage = new Page();
		m_pFile->GetPage(m_pCurrPage, ++m_nCurrPage);
	}

	if (m_pCurrPage)
	{
		// Try to fetch the first record from current_page
		// This function will delete this record from the page
		int ret = m_pCurrPage->GetFirst(&fetchme);
		if (!ret)
		{
			// Check if pages are still left in the file
			if (m_nCurrPage < m_nTotalPages)
			{
				// page ran out of records, so fetch next page
				delete m_pCurrPage;
				m_pCurrPage = new Page();
				m_pFile->GetPage(m_pCurrPage, ++m_nCurrPage);
				ret = m_pCurrPage->GetFirst(&fetchme);
				if (!ret) // failed to fetch next record
				{
					// check if we have reached the end of file
					if (m_nCurrPage >= m_nTotalPages)
					{
						// end of file reached
						return RET_FAILURE;
					}
					else
					{
						// end of file not reached, but record not found? fatal error!
						return RET_FAILURE;
					}
				}
			}
			else
			{
				// end of file reached
				return RET_FAILURE;
			}
		}
		// Record fetched successfully
		m_nCurrRecord++;
		return RET_SUCCESS;
	}
	else
	{
		// m_nCurrPage cannot be NULL at this point <-- FATAL error
		return RET_FAILURE;
	}

	return RET_SUCCESS;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
	WritePageToFile();
	return RET_SUCCESS;
}

void DBFile::WritePageToFile()
{
	m_pFile->AddPage(m_pPage, m_nTotalPages + 1);
    delete m_pPage;
    m_pPage = new Page();
    m_nTotalPages++;
}

