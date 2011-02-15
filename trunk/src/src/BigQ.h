#ifndef BIGQ_H
#define BIGQ_H

#include <pthread.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Defs.h"
#include "DBFile.h"

using namespace std;

// class to store run information
class Run
{
//private:
public:
    Page *pPage;	// current page from the run
    int m_nCurrPage; // page number wrt whole file, needed to fetch next page using m_runFile.GetPage()
	int m_nPagesFetched; // keep track of how many pages have been fetched from this run
	int  m_nRunLen; // run length of TPMMS

public:
    Run(int nRunLen)
    {
        pPage = new Page();
        m_nCurrPage = 0;
        m_nPagesFetched = 0;
        m_nRunLen = m_nRunLen;
    }

    ~Run()
    {
        delete pPage;
        pPage = NULL;
        m_nCurrPage = 0;
        m_nPagesFetched = 0;
        m_nRunLen = 0;
    }

    bool canFetchPage()
    {
        // more pages can be fetched from this run
        if (m_nPagesFetched < m_nRunLen)
            return true;
        return false;
    }

    Page * getPage()
    {
        return pPage;
    }

    // setPage() method needed...
};

class Record_n_Run
{
private:
	OrderMaker *m_pSortOrder;
	static ComparisonEngine *m_pCE;
	Record * m_pRec;
	int m_nRun;

public:

	Record_n_Run(OrderMaker *pOrderMaker, Record *rec, int run)
		: m_pSortOrder(pOrderMaker), m_pRec(rec), m_nRun(run)
	{}

	~Record_n_Run() {}

	Record * get_rec()
	{
		return m_pRec;
	}

	int get_run()
	{
		return m_nRun;
	}

	OrderMaker * get_order()
	{
		return m_pSortOrder;
	}

	ComparisonEngine * get_CE()
	{
		return m_pCE;
	}

	/*bool operator() (Record * r1, Record * r2)
	{
		if(m_pCE->Compare(r1, r2, m_pSortOrder) > 0)
			return true;
		return false;
	}*/

	// here or global?
	/*bool operator > (Record * r)
	{
		if(m_pCE->Compare(m_pRec, r, m_pSortOrder) > 0)
			return true;
		return false;
	}*/
};

class BigQ
{
private:
	DBFile m_runFile;
	Pipe *m_pInPipe, *m_pOutPipe;
	OrderMaker *m_pSortOrder;
	int m_nRunLen, m_nPageCount;
	string m_sFileName;
	ComparisonEngine ce;

private:
	// -------- phase - 1 --------------
	void swap(Record*&, Record*&);
	void appendRunToFile(vector<Record*>&);
	void quickSortRun(vector<Record*>& aRun, int begin, int end, ComparisonEngine &ce);
	int partition(vector<Record*>&, int, int, ComparisonEngine&);
	void* getRunsFromInputPipe();
	static void* getRunsFromInputPipeHelper(void*);

        //to compare records
        bool operator()(Record* const& r1, Record* const& r2)
        {
            Record* r11 = const_cast<Record*>(r1);
            Record* r22 = const_cast<Record*>(r2);
            if(ce.Compare(r11, r22, m_pSortOrder) <= 0)    //i.e. records are already sorted (r1 <= r2)
                return true;
            else
                return false;
        }

	struct CompareMyRecords
	{
	    CompareMyRecords(BigQ& self_):self(self_){}

            bool operator()(Record* const& r1, Record* const& r2)
            {
                Record* r11 = const_cast<Record*>(r1);
                Record* r22 = const_cast<Record*>(r2);
                if(self.ce.Compare(r11, r22, self.m_pSortOrder) <= 0)    //i.e. records are already sorted (r1 <= r2)
                    return true;
                else
                    return false;
            }

        BigQ& self;
	};

	// -------- phase - 2 --------------
	vector<Run *> m_vRuns;  // max size of this vector will be m_nPageCount/m_nRunLen
    int MergeRuns();
	bool MarkRunOver(int runNum);
	unsigned long int m_nRunOverMarker;
	void setupRunOverMarker();
	bool isRunAlive(int runNum);

public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
