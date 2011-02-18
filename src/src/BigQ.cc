#include "BigQ.h"
#include "math.h"
#include <queue>

using namespace std;

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
	: m_runFile(), m_nRunLen(runlen), m_nPageCount(1), m_sFileName()
{
    //init data structures
    m_pInPipe = &in;
    m_pOutPipe = &out;
    m_pSortOrder = &sortorder;
    m_sFileName = "runFile";
    m_runFile.Create(const_cast<char*>(m_sFileName.c_str()), heap, NULL);

#ifdef _DEBUG
    m_pSortOrder->Print();
#endif
    //TODO:
    //Arpit - change this bit manipulation approach to something more generic
    //which can accomodate any m-way merge

    // read data from in pipe sort them into runlen pages
    pthread_t sortingThread;
    pthread_create(&sortingThread, NULL, &getRunsFromInputPipeHelper, (void*)this);
    
}

BigQ::~BigQ ()
{
    // loop over m_vRuns and empty it out
}

void* BigQ::getRunsFromInputPipeHelper(void* context)
{
    ((BigQ *)context)->getRunsFromInputPipe();
}

void* BigQ::getRunsFromInputPipe()
{
    Record recFromPipe;
    vector<Record*> aRunVector;
    Page currentPage;
    int pageCountPerRun = 0;
    bool allSorted = true;
    bool overflowRec = false;

	int recs = 0;

    while(m_pInPipe->Remove(&recFromPipe))
    {
		recs++;
        Record *copyRec = new Record();
        copyRec->Copy(&recFromPipe); //because currentPage.Append() would consume the record

        //initially pageCountPerRun is always less than m_nRunLen (as runLen can't be 0)
        if(!currentPage.Append(&recFromPipe))
        {
			cout << "\n\n record " << recs << " did not fit in page " << m_nPageCount
				 << "\n\n current pages in the run are " << pageCountPerRun <<endl;

            //page full, start new page and increase the page count both global and local
            m_nPageCount++;
            pageCountPerRun++;
            currentPage.EmptyItOut();
            currentPage.Append(&recFromPipe);
            if(pageCountPerRun >= m_nRunLen)
            {
                //this is the only check for pageCount >= runLen and is sufficient
                //because pageCount can never increase until a record spills over
                overflowRec = true;
                pageCountPerRun = 0;    //reset pageCountPerRun for next run as current run is full
            }
        }
        if(!overflowRec)
        {
            aRunVector.push_back(copyRec); //whether it's a new page or old page, just push back the copy onto vector
            allSorted = false;
            continue;   //don't sort i.e. go down until the run is full
        }

        //control here means one run is full, sort it and write to file
        //then insert the overflow Rec and start over

        sort(aRunVector.begin(), aRunVector.end(), CompareMyRecords(m_pSortOrder));
        appendRunToFile(aRunVector);

        //now clear the vector to begin for new run
        //pageRecord must have been reset already as run was full
        aRunVector.clear();
        allSorted = true;
        if(overflowRec)     //this would always be the case
        {
            aRunVector.push_back(copyRec);  //value in copyRec is still intact
            allSorted = false;
            overflowRec = false;
        }
    }

    //done with all records in pipe, if there is anything in vector
    //it should be sorted and written out to file
    if(!allSorted)
    {
        //sort the vector
        sort(aRunVector.begin(), aRunVector.end(), CompareMyRecords(m_pSortOrder));
        appendRunToFile(aRunVector);    //flush everything to file

#ifdef _DEBUG
//    //get the records from runfile and feed 'em to outpipe
//    m_runFile.Open(const_cast<char*>(m_sFileName.c_str()));
//    m_runFile.MoveFirst();
//    Record *rc = new Record();
//    int recCtr = 0;
//    while (m_runFile.GetNext(*rc) == 1)
//    {
//    m_pOutPipe->Insert(rc);
//            recCtr++;
//    }
//    cout << "\n\n Records sent to outpipe = " << recCtr <<endl;
//    m_runFile.Close();
#endif


    }

    //now call mergeRuns here!
    MergeRuns();
    m_pOutPipe->ShutDown();
}

void BigQ::appendRunToFile(vector<Record*>& aRun)
{
    static int appendCount = 0;
#ifdef _DEBUG
//    cout<<"Append Run to File count : "<< ++appendCount<<endl;
#endif
    m_runFile.Open(const_cast<char*>(m_sFileName.c_str()));     //open with the same name
    int length = aRun.size();
    //insert first record into new page so that a clear demarcation can be established
    //start this demarcation from 2nd run (don't do it for first run )
    int i = 0;
    if(appendCount > 0)
    {
        m_runFile.Add(*aRun[0], true);
        i = 1;
    }
    for(; i < length; i++)  //initial value of i is already set above
            m_runFile.Add(*aRun[i]);
    m_runFile.Close();
    appendCount++;
}


/* --------------- Phase-2 of TPMMS: MergeRuns() --------------- */

#ifndef LESS_OP
#define LESS_OP

//ComparisonEngine ce;
//ComparisonEngine * Record_n_Run::m_pCE = &ce;

//bool operator < (Record_n_Run r1, Record_n_Run r2)
//{
//    if(r1.get_CE()->Compare(r1.get_rec(), r2.get_rec(), r1.get_order()) < 0)
//        return true;
//    return false;
//}
#endif

/* Input parameters: None
 * Return type: RET_SUCCESS or RET_FAILURE
 * Function: push sorted records through outPipe
 */
int BigQ::MergeRuns()
{
    m_runFile.Close();
    m_runFile.Open((char*)m_sFileName.c_str());
	ComparisonEngine ce;
	int nPagesFetched = 0;
	int nRunsAlive = 0;

	// delete this
	int recs = 0;

    // we need to do an m-way merge
    // m = total pages/run length
    const int nMWayRun = ceil((double)m_nPageCount/m_nRunLen);

	cout << "\n\n M = " << nMWayRun << endl;

	// Priority queue that contains sorted records
	priority_queue < Record_n_Run, vector <Record_n_Run>,
					 less<vector<Record_n_Run>::value_type> > pqRecords;

//    priority_queue<Record*, vector<Record*>, CompareMyRecords(*this)> pqRecords;

    // ---- Initial setup ----
    // There are m_nRunLen pages in one run
    // so fetch 1st page from each run
    // and put them in m_vRuns vector (sized nMWayRun)

    int runHeadPage = 0;
    Run * pRun = NULL;
    Record * pRec = NULL;

    for (int i = 0; i < nMWayRun; i++)
    {
        pRun = new Run(m_nRunLen);
        pRun->set_curPage(runHeadPage);
        m_runFile.GetPage(pRun->getPage(), pRun->get_and_inc_pagecount());
		nPagesFetched++;
		nRunsAlive++;

		cout << "\n\n runHeadPage = " << runHeadPage
			 << "\n nPagesFetched = "<< nPagesFetched
			 << "\n nRunsAlive = "<< nRunsAlive;

        // fetch 1st record and push in the priority queue
        pRec = new Record();
        int ret = pRun->getPage()->GetFirst(pRec);
        if (!ret)
        {
            // initially every page should have at least one record
            // error here... really bad!
            return RET_FAILURE;
        }

        if (pRec)
        {
            Record_n_Run rr(m_pSortOrder, &ce, pRec, i);
            pqRecords.push(rr);
            pRec = NULL;
        }
        else
        {
            // pRec cannot be NULL here... very wrong
            return RET_FAILURE;
        }

        // increment page-counter to go to the next run
        runHeadPage += m_nRunLen;
        m_vRuns.push_back(pRun);
    }

	cout << "\n\n m_vRuns.size() = " << m_vRuns.size()
		 << "\n pqRecords.size() = " << pqRecords.size() << endl;
    // fetch 1st record from each page
    // and push it in the min priority queue
    bool bFileEmpty = false;
    int nRunToFetchRecFrom = 0;
    while (bFileEmpty == false)
    {
        if (pqRecords.size() < nRunsAlive)
        {
            pRec = new Record;
            int ret = m_vRuns.at(nRunToFetchRecFrom)->getPage()->GetFirst(pRec);
            if (!ret)
            {
                // records from this page are over
                // see if new page from this run can be fetched
                if (m_vRuns.at(nRunToFetchRecFrom)->canFetchPage(m_nPageCount))
                {
                    // fetch next page
                    m_runFile.GetPage(m_vRuns.at(nRunToFetchRecFrom)->getPage(),
                                      m_vRuns.at(nRunToFetchRecFrom)->get_and_inc_pagecount());
					nPagesFetched++;
					cout << "\n\ncanFetchPage is true for run " << nRunToFetchRecFrom
						 << "\n so nPagesFetched = " << nPagesFetched <<endl;

                    // fetch first record from this page now
                    int ret2 = m_vRuns.at(nRunToFetchRecFrom)->getPage()->GetFirst(pRec);
                    if (!ret2)
                    {
                        cout << "\nBigQ::MergeRuns --> fetching record from page x of run "
                             << nRunToFetchRecFrom << " failed. Fatal!\n\n";
                        return RET_FAILURE;
                    }
                }
                else
                {
                    // all the pages from this run have been fetched
                    // size of M in the m-way run would reduce by one.. logically
					nRunsAlive--;
					cout << "\n\ncanFetchPage is *false* for run " << nRunToFetchRecFrom
						 << "\n--- nPagesFetched = "<< nPagesFetched
            			 << "\n--- nRunsAlive = "<< nRunsAlive;

					if (nPagesFetched == m_nPageCount && nRunsAlive == 0)
						bFileEmpty = true;

                    if (bFileEmpty == false)
                    {
                            // this run is over, fetch next record from the next alive run
                            nRunToFetchRecFrom = 0;
							while (nRunToFetchRecFrom < nMWayRun)
							{
								if (m_vRuns.at(nRunToFetchRecFrom)->is_alive())
									break;
								nRunToFetchRecFrom++;
							}
							if (nRunToFetchRecFrom == nMWayRun)
							{
								// File still has pages, still can't fine run - ERROR
								return RET_FAILURE;
							}
                    }
					delete pRec;
					pRec = NULL;	// because no record was fetched
                }
            }

            // got the record, push it in the min priority queue
            // for now, push it in a vector
            if (pRec)
            {
				Record_n_Run rr(m_pSortOrder, &ce, pRec, nRunToFetchRecFrom);
		        pqRecords.push(rr);
                pRec = NULL;
            }
        }

        // priority queue is full, pop min record
        if (nRunsAlive != 0 && pqRecords.size() == nRunsAlive)
        {
            // find min record
            Record_n_Run rr = pqRecords.top();
			pqRecords.pop();
            // push min element through out-pipe
            m_pOutPipe->Insert(rr.get_rec());
            // keep track of which run this record belonged too
            // need to fetch next record from the run of that page
            nRunToFetchRecFrom = rr.get_run();
            // do not delete memory allocated for record,
			recs++;
        }
    }

	cout << "\n\n records outed till now = "<< recs;

    // empty the priority queue
    while (pqRecords.size() > 0)
    {
		// find min record
        Record_n_Run rr = pqRecords.top();
		pqRecords.pop();
        // push min element through out-pipe
		m_pOutPipe->Insert(rr.get_rec());
        // keep track of which run this record belonged too
        // need to fetch next record from the run of that page
        nRunToFetchRecFrom = rr.get_run();
        // do not delete memory allocated for record,
		recs++;
    }
	cout << "\n\n records outed till now = "<< recs;

    return RET_SUCCESS;
}

