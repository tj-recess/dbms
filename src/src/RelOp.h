#ifndef REL_OP_H
#define REL_OP_H

#include <pthread.h>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	 pthread_t m_thread;
	// Record *buffer;
            struct Params
            {
                DBFile *inputFile;
                Pipe *outputPipe;
                CNF *selectOp;
                Record *literalRec;

                Params(DBFile *inFile, Pipe *outPipe, CNF *selOp, Record *literal)
                {
                    inputFile = inFile;
                    outputPipe = outPipe;
                    selectOp = selOp;
                    literalRec = literal;
                }
            };
            static void* DoOperation(void*);

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};

class Project : public RelationalOp 
{
	private:
		pthread_t m_thread;
        struct Params
        {
            Pipe *inputPipe, *outputPipe;
			int numAttsToKeep, numAttsOriginal;
	        int * pAttsToKeep;

            Params(Pipe *inPipe, Pipe *outPipe, int *keepMe,
				   int numAttsInput, int numAttsOutput)
            {
				inputPipe = inPipe;
                outputPipe = outPipe;
				numAttsToKeep = numAttsOutput;
				numAttsOriginal = numAttsInput;
				pAttsToKeep = keepMe;
            }
        };
        static void* DoOperation(void*);

	public:
		void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
		void WaitUntilDone ();
		void Use_n_Pages (int n) { }
};
class Join : public RelationalOp { 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class DuplicateRemoval : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};

class Sum : public RelationalOp 
{
	private:
		pthread_t m_thread;
        struct Params
        {
            Pipe *inputPipe, *outputPipe;
			Function *computeFunc;

            Params(Pipe *inPipe, Pipe *outPipe, Function *computeMe)
            {
                inputPipe = inPipe;
                outputPipe = outPipe;
				computeFunc = computeMe;
            }
        };
        static void* DoOperation(void*);

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { }
};

class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};

class WriteOut : public RelationalOp 
{
	private:
        pthread_t m_thread;
        struct Params
        {
            Pipe *inputPipe;
			Schema *pSchema;
			FILE *pFILE;

            Params(Pipe *inPipe, Schema *pMySchema, FILE *outFile)
            {
                inputPipe = inPipe;
				pSchema = pMySchema;
				pFILE = outFile;
            }
        };
        static void* DoOperation(void*);

	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { }
};
#endif