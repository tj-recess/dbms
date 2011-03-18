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


/* Input: inPipe = fetch input records from here
 * 		  outFile = File where text version should be written
 *					Its has been opened already
 *					TODO: caller doesn't close it, should we?
 *		  mySchema = the schema to use for writing records			
 */
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
	// Initialize member variables
	m_pInPipe = &inPipe;
	m_pSchema = &mySchema;
	m_pFILE = outFile;
	
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, &WriteOutHelper, (void*)this);
	
	return;
}

void * WriteOut::WriteOutHelper(void * context)
{
	((WriteOut*)context)->WriteOutOperation();
}

void WriteOut::WriteOutOperation()
{
	Record rec;	
	// While records are coming from inPipe, 
	// modify records and keep only desired attributes
	// and push the modified records into outPipe
	while(m_pInPipe->Remove(&rec))
	{
		// Basically copy the code from Record.cc (Record::Print function)
	    int n = m_pSchema->GetNumAtts();
	    Attribute *atts = m_pSchema->GetAtts();

	    // loop through all of the attributes
    	for (int i = 0; i < n; i++) 
		{

	        // print the attribute name
			fprintf(m_pFILE, "%s: ", atts[i].name);
    	    //cout << atts[i].name << ": ";

	        // use the i^th slot at the head of the record to get the
    	    // offset to the correct attribute in the record
        	int pointer = ((int *) rec.bits)[i + 1];

	        // here we determine the type, which given in the schema;
    	    // depending on the type we then print out the contents
			fprintf(m_pFILE, "[");
        	//cout << "[";

	        // first is integer
    	    if (atts[i].myType == Int) 
			{
        	    int *myInt = (int *) &(rec.bits[pointer]);
				fprintf(m_pFILE, "%d", *myInt);
            	//cout << *myInt;
		        // then is a double
        	} 
			else if (atts[i].myType == Double) 
			{
	            double *myDouble = (double *) &(rec.bits[pointer]);
				fprintf(m_pFILE, "%f", *myDouble);
    	        //cout << *myDouble;

        		// then is a character string
	        } 
			else if (atts[i].myType == String) 
			{
            	char *myString = (char *) &(rec.bits[pointer]);
				fprintf(m_pFILE, "%s", myString);
	            //cout << myString;
	        }
			fprintf(m_pFILE, "]");
	        //cout << "]";

    	    // print out a comma as needed to make things pretty
        	if (i != n - 1) 
			{
				fprintf(m_pFILE, ", ");
            	//cout << ", ";
	        }
    	}		// end of for loop
		fprintf(m_pFILE, "\n");
	    //cout << "\n";
	}			// no more records in the inPipe
}

void WriteOut::WaitUntilDone()
{
	// Block until thread is done
	pthread_join(m_thread, 0);
}


