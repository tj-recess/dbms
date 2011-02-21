#ifndef GENERIC_DBFILE_H
#define GENERIC_DBFILE_H

#include "DBFile.h"

class GenericDBFile
{
    private:
        string m_sFilePath; // path of the .bin file
        File *m_pFile;      // .bin file where data will be loaded
        Page *m_pPage;
        int m_nTotalPages;
        bool m_bDirtyPageExists;
        bool m_bIsDirtyMetadata;
        // Member variable to handle GetNext()
        int  m_nCurrPage;

        // Private member functions
        void WritePageToFile();
        int FetchNextRec(Record &fetchme, Page**, int&);

        // This method is currently unused. But will be used later
        //void WriteMetaData();

    public:
        GenericDBFile();
        ~GenericDBFile();

        // name = location of the .bin file
        // return value: 1 on success, 0 on failure
        int Create (char *name, void *startup);

        // This function assumes that the GenericDBFile already exists
        // and has previously been created and then closed.
        int Open (char *name);

        // Closes the file. 
        // The return value is a 1 on success and a zero on failure
        int Close ();

        // Forces the pointer to correspond to the first record in the file
        void MoveFirst();

        // Add new record to the end of the file
        // Note: addMe is consumed by this function and cannot be used again
        void Add (Record &addMe, bool startFromNewPage = false);

        // Fetch next record (relative to p_currPtr) into fetchMe
        int GetNext (Record &fetchMe);

		// Given a page number "whichPage", fetch that page
        inline void GetPage(Page *putItHere, off_t whichPage)
		{
			m_pFile->GetPage(putItHere, whichPage);
		}
		
		// Return total pages in the file
        inline int GetFileLength()
        {
            return m_pFile->GetLength();
        }
};

#endif
