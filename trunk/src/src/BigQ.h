#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Defs.h"
#include "DBFile.h"

using namespace std;

class BigQ {
	DBFile m_runFile;
	Pipe *m_pInPipe, *m_pOutPipe;
	OrderMaker *m_pSortOrder;
	int m_nRunLen, m_nPageCount;
	void quickSortRun(Record**, int, int, ComparisonEngine&);
	int partition(Record**, int, int, ComparisonEngine&);
	void swap(Record*&, Record*&);
	void appendRunToFile(vector<Record*>&);
	void quickSortRun(vector<Record*>& aRun, int begin, int end, ComparisonEngine &ce);
	int partition(vector<Record*>&, int, int, ComparisonEngine&);
	void* getRunsFromInputPipe();
	static void* getRunsFromInputPipeHelper(void*);
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
