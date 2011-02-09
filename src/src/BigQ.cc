#include "BigQ.h"

void* getRunsFromInputPipe(void*)
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

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
	: m_runFile(), m_nRunLen(runlen)
{

	//init data structures
	m_pInPipe = &in;
	m_pOutPipe = &out;
	m_pSortOrder = &sortorder;
	m_runFile.Create(const_cast<char*>("runFile"), heap, NULL);

	// read data from in pipe sort them into runlen pages
	pthread_t sortingThread;
	pthread_create(&sortingThread, NULL, getRunsFromInputPipe, NULL);

    // construct priority queue over sorted runs and dump sorted data
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ (){}



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
