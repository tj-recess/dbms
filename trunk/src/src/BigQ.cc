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
	// NOTE (Malvika): if we go by the bit-manipulation approach
	// we cannot handle runLen > 64
	// so error out here or something....

	// read data from in pipe sort them into runlen pages
	pthread_t sortingThread;
	pthread_create(&sortingThread, NULL, &getRunsFromInputPipeHelper, (void*)this);

    // construct priority queue over sorted runs and dump sorted data
 	// into the out pipe

    // finally shut down the out pipe
	// out.ShutDown ();
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
//	int pageCount = 0;
	bool allSorted = true;
        ComparisonEngine ce;

	while(m_pInPipe->Remove(&recFromPipe))
	{
            if(m_nPageCount <= m_nRunLen)
            {
                Record *copyRec = new Record();
                copyRec->Copy(&recFromPipe); //because currentPage.Append() would consume the record
                if(!currentPage.Append(&recFromPipe))
                {
                    //page full, start new page and increase the page count
                    m_nPageCount++;
                    currentPage.EmptyItOut();
                    currentPage.Append(&recFromPipe);
                }
                aRunVector.push_back(copyRec); //whether it's a new page or old page, just push back the copy onto vector
                allSorted = false;
            }
            else
            {
                //one run is full, sort it and write to file
                //then fetch the data for next run

                //sort the records inside aRun
                quickSortRun(aRunVector, 0, aRunVector.size(), ce);

                //write the sorted run onto disk in runFile
                appendRunToFile(aRunVector);

                //now clear the vector to begin for new run
                aRunVector.clear();
                allSorted = true;
            }
	}

	//done with all records in pipe, if there is anything in vector
	//it should be sorted and written out to file
	if(!allSorted)
	{
            //sort the vector
	    sort(aRunVector.begin(), aRunVector.end(), CompareMyRecords(*this));

//            for(int i = 0; i < aRunVector.size(); i++)
//                m_pOutPipe->Insert(aRunVector[i]);

	    //quickSortRun(aRunVector, 0, aRunVector.size(), ce);

#ifdef _DEBUG
            int vectorLen = aRunVector.size();
#endif
            appendRunToFile(aRunVector);//check would be done internally in function
#ifdef _DEBUG
            //get the records from runfile and feed 'em to outpipe
//            m_runFile.MoveFirst();
//            for(int i = 0; i < vectorLen; i++)
//            {
//                Record *rc = new Record();
//                m_runFile.GetNext(*rc);
//                m_pOutPipe->Insert(rc);
//            }
#endif
	}

        //now call mergeRuns here!
        MergeRuns();
        m_pOutPipe->ShutDown();
}

void BigQ::swap(Record*& a, Record*& b)
{
	Record* temp = a;
	a = b;
	b = temp;
}

void BigQ::appendRunToFile(vector<Record*>& aRun)
{
    m_runFile.Open(const_cast<char*>(m_sFileName.c_str()));     //open with the same name
    int length = aRun.size();
    for(int i = 0; i < length; i++)
            m_runFile.Add(*aRun[i]);
    m_runFile.Close();
}


void BigQ::quickSortRun(vector<Record*>& aRun, int begin, int end, ComparisonEngine &ce)
{
	if(begin < end)
	{
		int split = partition(aRun, begin, end, ce);
		quickSortRun(aRun, begin, split, ce);
		quickSortRun(aRun, split + 1, end, ce);
	}
}

int BigQ::partition(vector<Record*>& aRun, int begin, int end, ComparisonEngine &ce)
{
	Record *pivot = aRun[begin];
	int i = begin + 1;
	for (int j = begin + 1; j < end; j++)
	{
		if(ce.Compare(pivot, aRun[j], m_pSortOrder) > 0)	//i.e. pivot is bigger than aRun[j]
		{
			//swap aRun[i] and aRun[j] and increment i
			swap(aRun[i], aRun[j]);

			i++;
		}
	}
	//now swap pivot and (i-1)th element (which is smaller than pivot)
	swap(pivot, aRun[i-1]);
	return i;
}

/* ----- Phase-2 of TPMMS: MergeRuns() -----
 *
 * input parameters:
 * output parameters:
 * return type

/* --------------- Phase-2 of TPMMS: MergeRuns() --------------- */

#ifndef LESS_OP
#define LESS_OP

ComparisonEngine ce;
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
	setupRunOverMarker();

    // we need to do an m-way merge
    // m = total pages/run length
    const int nMWayRun = ceil((double)m_nPageCount/m_nRunLen);

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
        pRun->m_nCurrPage = runHeadPage;
        m_runFile.GetPage(pRun->pPage, pRun->m_nCurrPage++);

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
            Record_n_Run rr(m_pSortOrder, pRec, i);
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


    // fetch 1st record from each page
    // and push it in the min priority queue
    bool bFileEmpty = false;
    int nRunToFetchRecFrom = 0;
    while (bFileEmpty == false)
    {
        if (pqRecords.size() < nMWayRun)
        {
            pRec = new Record;
            int ret = m_vRuns.at(nRunToFetchRecFrom)->getPage()->GetFirst(pRec);
            if (!ret)
            {
                // records from this page are over
                // see if new page from this run can be fetched
                if (m_vRuns.at(nRunToFetchRecFrom)->canFetchPage())
                {
                    // fetch next page
                    m_runFile.GetPage(m_vRuns.at(nRunToFetchRecFrom)->pPage,
                                     m_vRuns.at(nRunToFetchRecFrom)->m_nCurrPage++);
                    // fetch first record from this page now
                    ret = m_vRuns.at(nRunToFetchRecFrom)->getPage()->GetFirst(pRec);
                    if (!ret)
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

                    // mark the fact that this run is over
                    // if all runs are over, funtion will return true
                    bFileEmpty = MarkRunOver(nRunToFetchRecFrom);

                    if (bFileEmpty == false)
                    {
                            // this run is over, fetch next record from the next alive run
                            nRunToFetchRecFrom = 0;
                            bool bRunFound = false;
                            while (bRunFound == false)
                            {
                                    bRunFound = isRunAlive(nRunToFetchRecFrom);
                                    if (bRunFound)
                                            break;
                                    nRunToFetchRecFrom++;
                            }
                    }
                }
            }

            // got the record, push it in the min priority queue
            // for now, push it in a vector
            if (pRec)
            {
		//pair<Record*, int> recordRunPair(pRec, nRunToFetchRecFrom);
                //vPQRecords.push_back(recordRunPair);
		Record_n_Run rr(m_pSortOrder, pRec, nRunToFetchRecFrom);
	        pqRecords.push(rr);
                pRec = NULL;
            }
        }

        // priority queue is full, pop min record
        if (pqRecords.size() == nMWayRun)
        {
            // find min record
            Record_n_Run rr = pqRecords.top();
            // push min element through out-pipe
            m_pOutPipe->Insert(rr.get_rec());
            // keep track of which run this record belonged too
            // need to fetch next record from the run of that page
            nRunToFetchRecFrom = rr.get_run();
            // do not delete memory allocated for record,
        }
    }

    // empty the priority queue
    while (pqRecords.size() > 0)
    {
		// find min record
        Record_n_Run rr = pqRecords.top();
        // push min element through out-pipe
		m_pOutPipe->Insert(rr.get_rec());
        // keep track of which run this record belonged too
        // need to fetch next record from the run of that page
        nRunToFetchRecFrom = rr.get_run();
        // do not delete memory allocated for record,
    }

    return RET_SUCCESS;
}

// Mark that the run "runNum" is over. That is, all the pages from this run
// have been fetched. Run over --> bit = 1
// If all the runs are over, return true --> whole file has been read
bool BigQ::MarkRunOver(int runNum)
{
	unsigned long int bitRunOver = 0x00000001;

	// left shift 1 by runNum bits
	bitRunOver << runNum;

	m_nRunOverMarker = m_nRunOverMarker | bitRunOver;
	if (m_nRunOverMarker == 0xFFFFFFFF)
		return true;

	return false;
}

// we do not need to consider the bits that do not corrospond to runs
// those would be leftmost bits, so set them to 1
void BigQ::setupRunOverMarker()
{
	unsigned long int mask = 0xFFFFFFFF;
	// insert m_nRunLen 0s on the right
	// for 4 runs, m_nRunOverMarker = 0xFFF0
	// for 5 runs, m_nRunOverMarker = 0xFFE0
	mask << m_nRunLen;

	m_nRunOverMarker = mask;
}

// Check if the given run is alive
// return true if it is, else false
bool BigQ::isRunAlive(int runNum)
{
	unsigned long int bitForRun = 0x00000001;
    bitForRun << runNum;
	unsigned long int result = m_nRunOverMarker & bitForRun;
	if (result == 0)
		return true;

	return false;
}
