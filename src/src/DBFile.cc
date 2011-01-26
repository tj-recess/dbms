#include "DBFile.h"

DBFile::DBFile(): m_sFilePath(), m_pCurrPage(NULL), m_nTotalPages(0),
				  m_nCurrPage(0), m_nCurrRecord(0), m_bDirtyPageExists(false),
				  m_bIsDirtyMetadata(false)
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

	// open file in append mode, preserving all prev content
	if (m_pFile)
	{
		m_pFile->Open(APPEND, const_cast<char*>(fname));//openInAppendMode
	}
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
	Open(const_cast<char*>(m_sFilePath.c_str()));

	/*
	 * logic : first read the record from the file using suckNextRecord()
	 * then add this record to page and keep adding such records until page is full
	 * once page is full, write it to file.
	 * Or after we finish the loop, write to file before leaving from this function
	 */

	Record aRecord;

	while(aRecord.SuckNextRecord(&mySchema, fileToLoad))
	{
		if(!m_pPage->Append(&aRecord))	//if append fails then write page to disk
		{
			WritePageToFile();
			m_pPage->Append(&aRecord);//also put this already sucked record into the new page
		}
		// set dirty page to true
		if (!m_bDirtyPageExists)
			m_bDirtyPageExists = true;
	}
	WritePageToFile();
}

void DBFile::Add (Record &rec)
{
	Record aRecord;
	aRecord.Consume(&rec);

	// fetch last used page into m_pPage
	if(m_pPage)
	{
		//TODO check if we need to write back before deleting the page directly
		delete m_pPage;
	}
	m_pFile->GetPage(m_pPage, m_nTotalPages);

	// Try to store the record into current page
	m_bDirtyPageExists = true;
	if (!m_pPage->Append(&aRecord))	// current page does not have enough space
	{
		// write current page to file
		// this function will create a new page too
		WritePageToFile();
		if (!m_pPage->Append(&aRecord))
		{
			//TODO Fatal Error
		}
	}
}

void DBFile::MoveFirst ()
{
	// Reser current page and record pointers
	m_nCurrPage = 0; // TODO: or -1?
	m_nCurrRecord = 0;
	delete m_pCurrPage;
	m_pCurrPage = NULL;
}

int DBFile::GetNext (Record &fetchme)
{
	// write dirty page to file
	// as it might happen that the record we want to fetch now
	// is still in the dirty page which has not been flushed to disk
	WritePageToFile();

	// coming for the first time
	// Page starts with 0, but data is stored from 1st page onwards
	// Refer to File :: GetPage (File.cc line 168)
	if (m_nCurrPage == 0)
	{
		// Store a copy of the page in member buffer
		m_pCurrPage = new Page();
		m_pFile->GetPage(m_pCurrPage, m_nCurrPage++);
	}

	if (m_pCurrPage)
	{
		// Try to fetch the first record from current_page
		// This function will delete this record from the page
		int ret = m_pCurrPage->GetFirst(&fetchme);
		if (!ret)
		{
			// Check if pages are still left in the file
			if (m_nCurrPage < m_pFile->GetLength()-1)	//first page in File doesn't store the data, so if GetLength() tells 2 pages,
			{											//data is actually stored in only one page.
				// page ran out of records, so fetch next page
				delete m_pCurrPage;
				m_pCurrPage = new Page();
				m_pFile->GetPage(m_pCurrPage, m_nCurrPage++);
				ret = m_pCurrPage->GetFirst(&fetchme);
				if (!ret) // failed to fetch next record
				{
					// check if we have reached the end of file
					if (m_nCurrPage >= m_pFile->GetLength())
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
	/*
	 * logic :
	 * first read the record from the file in "fetchme,
	 * pass it for comparison using given cnf and literal.
	 * if compEngine.compare returns 1, it means fetched record
	 * satisfies CNF expression so we simple return success (=1) here
	 */

	ComparisonEngine compEngine;

	while(GetNext(fetchme)) 	/*GetNext(Record&) handles dirty pages possibility*/
	{
		if(compEngine.Compare(&fetchme, &literal, &cnf))
			return RET_SUCCESS;
	}

	//if control is here then no matching record was found
	return RET_FAILURE;
}

void DBFile::WritePageToFile()
{
	if (m_bDirtyPageExists)
	{
		m_pFile->AddPage(m_pPage, m_nTotalPages);
		delete m_pPage;
		m_pPage = new Page();
		m_nTotalPages++;
	}
    m_bDirtyPageExists = false;
}

void DBFile::WriteMetaData()
{
   if(m_bIsDirtyMetadata && !m_sFilePath.empty())
   {
	   ofstream out;
	   out.open(string(m_sFilePath + "meta.data").c_str(),ios::trunc);
	   out<<m_nTotalPages;
	   out.close();
   }
}
