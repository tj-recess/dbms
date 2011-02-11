#include "BigQ.h"
#include "math.h"

using namespace std;

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
	: m_runFile(), m_nRunLen(runlen), m_nPageCount(0), m_sFileName()
{

	//init data structures
	m_pInPipe = &in;
	m_pOutPipe = &out;
	m_pSortOrder = &sortorder;
	m_sFileName = "runFile";
    m_runFile.Create(const_cast<char*>(m_sFileName.c_str()), heap, NULL);

	// read data from in pipe sort them into runlen pages
	pthread_t sortingThread;
	pthread_create(&sortingThread, NULL, &getRunsFromInputPipeHelper, (void*)this);

    // construct priority queue over sorted runs and dump sorted data
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
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
	Record *recFromPipe;
	vector<Record*> aRunVector;

	while(m_pInPipe->Remove(recFromPipe))
	{
		if(aRunVector.size() < m_nRunLen)
		{
			aRunVector.push_back(recFromPipe);
		}
		else
		{
			//sort the records inside aRun
			ComparisonEngine ce;
			quickSortRun(aRunVector, 0, aRunVector.size(), ce);

			//write the sorted run onto disk in runFile
			appendRunToFile(aRunVector);

			//now clear the vector to begin for new run
			aRunVector.clear();
		}
	}

	//done with all records in pipe, if there is anything in vector
	//it should be written out to file
	appendRunToFile(aRunVector);//check would be done internally in function

	//now call mergeRuns here!
}


void BigQ::swap(Record*& a, Record*& b)
{
	Record* temp = a;
	a = b;
	b = temp;
}

void BigQ::appendRunToFile(vector<Record*>& aRun)
{
	int length = aRun.size();
	for(int i = 0; i < length; i++)
		m_runFile.Add(*aRun[i]);
}


void BigQ::quickSortRun(vector<Record*> &aRun, int begin, int end, ComparisonEngine &ce)
{
	if(begin < end)
	{
		int split = partition(aRun, begin, end, ce);
		quickSortRun(aRun, begin, split, ce);
		quickSortRun(aRun, split + 1, end, ce);
	}
}

int BigQ::partition(vector<Record*> &aRun, int begin, int end, ComparisonEngine &ce)
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
 */
int BigQ::MergeRuns()
{
    m_runFile.Close();
    m_runFile.Open((char*)m_sFileName.c_str());
	setupRunOverMarker();

    // we need to do an m-way merge
    // m = total pages/run length
    const int nMWayRun = ceil(m_nPageCount/m_nRunLen);
    vector < pair<Record*, int> > vPQRecords;

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
        int ret = m_vRuns.at(i)->getPage()->GetFirst(pRec);
        if (!ret)
        {
            // initially every page should have at least one record
            // error here... really bad!
            return RET_FAILURE;
        }

        if (pRec)
        {
			pair<Record*, int> recordRunPair(pRec, i);
            vPQRecords.push_back(recordRunPair);
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
        if (vPQRecords.size() < nMWayRun)
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
                        cout << "\nBigQ::MergeRuns --> fetching record from page x of "
                             << "run " << nRunToFetchRecFrom << " failed. Fatal!\n\n";
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
				pair<Record*, int> recordRunPair(pRec, nRunToFetchRecFrom);
                vPQRecords.push_back(recordRunPair);
                pRec = NULL;
            }
        }

        // priority queue is full, pop min record
        if (vPQRecords.size() == nMWayRun)
        {
            // find min record
            // push min element through out-pipe

            // ------
            // keep track of which run this record belonged too
            // need to fetch next record from the run of that page
            // update nRunToFetchRecFrom
			int min = 0;
			pair<Record*, int> recordRunPair = vPQRecords.at(min);
			nRunToFetchRecFrom = recordRunPair.second;
            // delete memory allocated for record, 
            // safe operation - bcoz data has been consumed by the out-pipe
            delete recordRunPair.first;
        }
    }

    // empty the priority queue
    while (vPQRecords.size() > 0)
    {
        // push min element through out pipe
        // delete pRec, first element in the pair

		int min = 0;
        pair<Record*, int> recordRunPair = vPQRecords.at(min);
        delete recordRunPair.first;
    }

    return RET_SUCCESS;
}

// Mark that the run "runNum" is over. That is, all the pages from this run
// have been fetched. Run over --> bit = 1
// If all the runs are over, return true --> whole file has been read
bool BigQ::MarkRunOver(int runNum)
{
	long int bitRunOver = 0x00000001;

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
	long int mask = 0xFFFFFFFF;
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
	long int bitForRun = 0x00000001;
    bitForRun << runNum;
	long int result = m_nRunOverMarker & bitForRun;
	if (result == 0)
		return true;

	return false;
}
