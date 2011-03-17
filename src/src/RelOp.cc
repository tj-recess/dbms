#include "RelOp.h"

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
}

void SelectFile::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}


/* Input: inPipe = fetch input records from here
 *	      outPipe = push project output here
 *        keepMe = array containing the attributes to project eg [3,5,2,7]
 *        numAttsInput = Total atts in input rec
 *        numAttsOutput = Atts to keep in output rec 
 *                        i.e. length of int * keepMe array
 */
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, 
				   int numAttsInput, int numAttsOutput)
{
	// Initialize member variables
	m_pInPipe = &inPipe;
	m_pOutPipe = &outPipe;
	m_pAttsToKeep = keepMe;
	m_nNumAttsToKeep = numAttsOutput;
	m_nNumAttsOriginal = numAttsInput;
	
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, &ProjectHelper, (void*)this);
	
	return;
}

void * Project::ProjectHelper(void * context)
{
	((Project*)context)->ProjectOperation();
}

void Project::ProjectOperation()
{
	Record rec;	
	// While records are coming from inPipe, 
	// modify records and keep only desired attributes
	// and push the modified records into outPipe
	while(m_pInPipe->Remove(&rec))
	{
		// Porject function will modify "rec" itself
		rec.Project(m_pAttsToKeep, m_nNumAttsToKeep, m_nNumAttsOriginal);
		// Push this modified rec in outPipe
		m_pOutPipe->Insert(&rec);
	}
	
	//Shut down the outpipe
	m_pOutPipe->ShutDown();
}

void Project::WaitUntilDone()
{
	// Block until thread is done
	pthread_join(m_thread, 0);
}
