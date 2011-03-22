#include "RelOp.h"

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
    pthread_create(&m_thread, NULL, DoOperation, 
				   (void*)new Params(&inFile, &outPipe, &selOp, &literal));
}

/*Logic:
 * Keep scanning the file using GetNext(..) and apply CNF (within GetNext(..),
 * insert into output pipe whichever record matches.
 * Shutdown the output Pipe
 */
void* SelectFile::DoOperation(void* p)
{
    Params* param = (Params*)p;
    param->inputFile->MoveFirst();
    Record rec;
#ifdef _OPS_DEBUG
    int cnt = 0;
#endif
    while(param->inputFile->GetNext(rec, *(param->selectOp), *(param->literalRec)))
    {
#ifdef _OPS_DEBUG
        cnt++;
#endif
        param->outputPipe->Insert(&rec);
    }
#ifdef _OPS_DEBUG
    cout<<"SelectFile : inserted " << cnt << " recs in output Pipe"<<endl;
#endif
    param->outputPipe->ShutDown();
	delete param;
	param = NULL;
}

void SelectFile::WaitUntilDone () {
	 pthread_join (m_thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen)
{
    /*Arpit - This method has no impact in this class, probably.
     we have it just because it's pure virtual in
     */
}

//int Join::m_nRunLen = -1;
int Join::m_nRunLen = 10;

void Join::Run(Pipe& inPipeL, Pipe& inPipeR, Pipe& outPipe, CNF& selOp, Record& literal)
{
    if (m_nRunLen == -1)
    {
        cerr << "\nError! Use_n_Page() must be called and "
             << "pages of memory allowed for operations must be set!\n";
        exit(1);
    }

    pthread_create(&thread, NULL, DoOperation, (void*)new Params(&inPipeL, &inPipeR, &outPipe, &selOp, &literal));
}

void* Join::DoOperation(void* p)
{
    Params* param = (Params*)p;
    /*actual logic goes here
     * 1. we have to create BigQL for inPipeL and BigQR for inPipeR
     * 2. Try to Create OrderMakers (omL and omR) for both BigQL and BigQR using CNF (use the GetSortOrder method)
     * 3. If (omL != null && omR != null) then go ahead and merge these 2 BigQs.(after constructing them of course)
     * 4. else - join these bigQs using "block-nested loops join" - block-size?
     * 5. Put the results into outPipe and shut it down once done.
     */
    OrderMaker omL, omR;
    param->selectOp->GetSortOrders(omL, omR);
	
	if (omL.numAtts == omR.numAtts && omR.numAtts > 0)
	{
        const int pipeSize = 100;
        Pipe outL(pipeSize), outR(pipeSize);
        BigQ bigqL(*(param->inputPipeL), outL, omL, m_nRunLen);
        BigQ bigR(*(param->inputPipeR), outR, omR, m_nRunLen);
        Record recL, recR;
		// fetch one leftRec and one righRec
		// find stuff for merging using them
		bool left_fetched = false;
		if (outL.Remove(&recL))
			left_fetched = true;

		bool right_fetched = false;
		if (outR.Remove(&recR))
			right_fetched = true;
	
		if (left_fetched && right_fetched)
        {
			// Logic:
			// <size of record><byte location of att 1><byte location of attribute 2>...<byte location of att n><att 1><att 2>...<att n>
			// num atts in rec = (byte location of att 1)/(sizeof(int)) - 1
			int left_tot = ((int *) recL.bits)[1]/sizeof(int) - 1;
			int right_tot = ((int *) recR.bits)[1]/sizeof(int) - 1;
			int numAttsToKeep = left_tot + right_tot;
			int attsToKeep[numAttsToKeep], attsToKeepLeft[omL.numAtts], attsToKeepRight[omR.numAtts];
		
			// make attsToKeepLeft
			for (int i = 0; i < omL.numAtts; i++)
				attsToKeepLeft[i] = omL.whichAtts[i];

			// make attsToKeepRight
			for (int i = 0; i < omR.numAtts; i++)
				attsToKeepRight[i] = omR.whichAtts[i];

			// make attsToKeep - for final merged/joined record
			// <all from left> + <all from right> 
        	int i;
	        for (i = 0; i < left_tot; i++)
    	        attsToKeep[i] = i;
			for (int j = 0; j < right_tot; j++)
				attsToKeep[i++] = j;

			// Make orderMaker for comparing records on the join-attributes
			OrderMaker JoinAttsOM;	
			JoinAttsOM.numAtts = omR.numAtts;
			for (int j = 0; j < omR.numAtts; j++)
			{
				JoinAttsOM.whichAtts[j] = j;
				JoinAttsOM.whichTypes[j] = omR.whichTypes[j];
			}

			// Porject grp-atts of left and right record
			// Compare them (make order maker)
			// if equal, join ... fetch only right (fetch from FK table)
			// if left < right, fetch next left
			// if left > right, fetch next right
	        Record joinResult, copy_left, copy_right;
			ComparisonEngine ce;
			bool bError = false;
			int cnt = 1;
			do
			{
		        copy_left.Copy(&recL);
		        copy_right.Copy(&recR);
				copy_left.Project(attsToKeepLeft, omL.numAtts, left_tot);
				copy_right.Project(attsToKeepRight, omR.numAtts, right_tot);
				// join attributes match, fetch from FK table (assume its right)
				int ret = ce.Compare(&copy_left, &copy_right, &JoinAttsOM);
				if (ret == 0)
				{
	    		    joinResult.MergeRecords(&recL, &recR, left_tot, right_tot, attsToKeep, numAttsToKeep, left_tot);
					#ifdef _OPS_DEBUG
					cout << "\n" << cnt++ <<" Result col:  " << ((int *) joinResult.bits)[1]/sizeof(int) - 1;
					#endif
        			param->outputPipe->Insert(&joinResult);

					if (!outR.Remove(&recR))
						bError = true;
				}
				// left is smaller than right, fetch new left
				else if (ret < 0)
				{
					if (!outL.Remove(&recL))
                        bError = true;
				}
				else	// left is greater than right, fetch right
				{
					if (!outR.Remove(&recR))
                        bError = true;
				}
		    } while(!bError);
        }

		// clear out pipes
		while (outR.Remove(&recR))
		{
			// do nothing
		}
		while (outL.Remove(&recL))
		{
			// do nothing
		}
		outL.ShutDown();
		outR.ShutDown();
	}
	else
	{
		// block-nested-loop-join
	}

    param->outputPipe->ShutDown();
}

void Join::WaitUntilDone () 
{
	 pthread_join (thread, NULL);
}

void Join::Use_n_Pages (int runlen)
{
    //not sure if this runLen should be halved as
    //2 BigQs will be constructed
    m_nRunLen = runlen;
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
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, &DoOperation, 
				   (void*) new Params(&inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput));
	
	return;
}

void * Project::DoOperation(void * p)
{
	Params* param = (Params*)p;
	Record rec;	
	// While records are coming from inPipe, 
	// modify records and keep only desired attributes
	// and push the modified records into outPipe
	while(param->inputPipe->Remove(&rec))
	{
		// Porject function will modify "rec" itself
		rec.Project(param->pAttsToKeep, param->numAttsToKeep, param->numAttsOriginal);
		// Push this modified rec in outPipe
		param->outputPipe->Insert(&rec);
	}
	
	//Shut down the outpipe
	param->outputPipe->ShutDown();
	delete param;
	param = NULL;
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
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, &DoOperation, 
				   (void*) new Params(&inPipe, &mySchema, outFile));
	
	return;
}

void * WriteOut::DoOperation(void * p)
{
	Params* param = (Params*)p;
	Record rec;	
	// While records are coming from inPipe, 
	// Write out the attributes in text form in outFile
	while(param->inputPipe->Remove(&rec))
	{
		// Basically copy the code from Record.cc (Record::Print function)
	    int n = param->pSchema->GetNumAtts();
	    Attribute *atts = param->pSchema->GetAtts();

	    // loop through all of the attributes
    	for (int i = 0; i < n; i++) 
		{

	        // print the attribute name
			fprintf(param->pFILE, "%s: ", atts[i].name);
    	    //cout << atts[i].name << ": ";

	        // use the i^th slot at the head of the record to get the
    	    // offset to the correct attribute in the record
        	int pointer = ((int *) rec.bits)[i + 1];

	        // here we determine the type, which given in the schema;
    	    // depending on the type we then print out the contents
			fprintf(param->pFILE, "[");
        	//cout << "[";

	        // first is integer
    	    if (atts[i].myType == Int) 
			{
        	    int *myInt = (int *) &(rec.bits[pointer]);
				fprintf(param->pFILE, "%d", *myInt);
            	//cout << *myInt;
		        // then is a double
        	} 
			else if (atts[i].myType == Double) 
			{
	            double *myDouble = (double *) &(rec.bits[pointer]);
				fprintf(param->pFILE, "%f", *myDouble);
    	        //cout << *myDouble;

        		// then is a character string
	        } 
			else if (atts[i].myType == String) 
			{
            	char *myString = (char *) &(rec.bits[pointer]);
				fprintf(param->pFILE, "%s", myString);
	            //cout << myString;
	        }
			fprintf(param->pFILE, "]");
	        //cout << "]";

    	    // print out a comma as needed to make things pretty
        	if (i != n - 1) 
			{
				fprintf(param->pFILE, ", ");
            	//cout << ", ";
	        }
    	}		// end of for loop
		fprintf(param->pFILE, "\n");
	    //cout << "\n";
	}			// no more records in the inPipe

	delete param;
	param = NULL;
}

void WriteOut::WaitUntilDone()
{
	// Block until thread is done
	pthread_join(m_thread, 0);
}


/* Input: inPipe = fetch input records from here
 *	      outPipe = push project output here
 *		  computeMe = Function using which sum must be computed
 */
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, &DoOperation, 
				   (void*) new Params(&inPipe, &outPipe, &computeMe));
	
	return;
}

void * Sum::DoOperation(void * p)
{
	Params* param = (Params*)p;
	Record rec;	
	double sum = 0;
	// While records are coming from inPipe, 
	// Use function on them and calculate sum
	while(param->inputPipe->Remove(&rec))
	{
		int ival = 0; double dval = 0;
		param->computeFunc->Apply(rec, ival, dval);
		sum += (ival + dval);
		#ifdef _OPS_DEBUG
			cout << "\nsum = "<< sum;
		#endif
	}

	// create temperory schema, with one attribute - sum
	Attribute att = {(char*)"sum", Double};

	Schema sum_schema((char*)"tmp_sum_schema_file", // filename, not used
					   1, 					 		// number of attributes	
				       &att);				 		// attribute pointer

	// Make a file that contains this sum
	FILE * sum_file = fopen("tmp_sum_data_file", "w");
	fprintf(sum_file, "%f|", sum);
	fclose(sum_file);
	sum_file = fopen("tmp_sum_data_file", "r");
	// Make record using the above schema and data file
	rec.SuckNextRecord(&sum_schema, sum_file);
	fclose(sum_file);

	// Push this record to outPipe
	param->outputPipe->Insert(&rec);

    // Shut down the outpipe
    param->outputPipe->ShutDown();

	// delete file "tmp_sum_data_file"
	if(remove("tmp_sum_data_file") != 0)
        perror("\nError in removing tmp_sum_data_file!");

    delete param;
    param = NULL;
}

void Sum::WaitUntilDone()
{
    // Block until thread is done
    pthread_join(m_thread, 0);
}

/* Input: inPipe = fetch input records from here
 *        outPipe = push project output here
 *        mySchema = Schema of the records coming in inPipe 
 */
//int DuplicateRemoval::m_nRunLen = -1;
int DuplicateRemoval::m_nRunLen = 10;

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	if (m_nRunLen == -1)
	{
		cerr << "\nError! Use_n_Page() must be called and "
			 << "pages of memory allowed for operations must be set!\n";
		exit(1);
	}
    // Create thread to do the project operation
    pthread_create(&m_thread, NULL, &DoOperation,
                   (void*) new Params(&inPipe, &outPipe, &mySchema));
    return;
}

void * DuplicateRemoval::DoOperation(void * p)
{
    Params* param = (Params*)p;
    Record lastSeenRec, currentRec;
	OrderMaker sortOrder;
	int pipeSize = 100;

	// make sort order maker for bigQ
    int n = param->pSchema->GetNumAtts();
    Attribute *atts = param->pSchema->GetAtts();
	// loop through all of the attributes, and list as it is
	sortOrder.numAtts = n;
    for (int i = 0; i < n; i++)
    {
		sortOrder.whichAtts[i] = i;
		sortOrder.whichTypes[i] = atts[i].myType;
	}

	// create local outPipe
	Pipe localOutPipe(pipeSize);
	// start bigQ
   	BigQ B(*(param->inputPipe), localOutPipe, sortOrder, m_nRunLen);

	bool bLastSeenRecSet = false;
	ComparisonEngine ce;
	while (localOutPipe.Remove(&currentRec))
	{
		if (bLastSeenRecSet == false)
		{
			lastSeenRec = currentRec;
			bLastSeenRecSet = true;
		}

		// compare currentRec with lastSeenRec
		// if same, do nothing
		// if different, send currentRec to param->outputPipe
		//				 and update lastSeenRec
		if (ce.Compare(&lastSeenRec, &currentRec, &sortOrder) != 0)
		{
			lastSeenRec = currentRec;
			param->outputPipe->Insert(&currentRec);
		}
	}

    //Shut down the outpipe
	localOutPipe.ShutDown();
    param->outputPipe->ShutDown();
	
    delete param;
    param = NULL;
}

void DuplicateRemoval::Use_n_Pages(int n)
{
	// set runLen for bigQ as the number of pages allowed for internal use
	m_nRunLen = n;
}

void DuplicateRemoval::WaitUntilDone()
{
    // Block until thread is done
    pthread_join(m_thread, 0);
}

void GroupBy::Use_n_Pages(int n)
{
    m_nRunLen = n;
}

void GroupBy::Run(Pipe& inPipe, Pipe& outPipe, OrderMaker& groupAtts, Function& computeMe)
{
    pthread_create(&m_thread, NULL, DoOperation, (void*)new Params(&inPipe, &outPipe, &groupAtts, &computeMe, m_nRunLen));
}

void GroupBy::WaitUntilDone()
{
    pthread_join(m_thread, 0);
}

void* GroupBy::DoOperation(void* p)
{
    Params* param = (Params*)p;
    //create a local outputPipe and a BigQ and an feed it with current inputPipe
    const int pipeSize = 100;
    Pipe localOutPipe(pipeSize);
    BigQ localBigQ(*(param->inputPipe), localOutPipe, *(param->groupAttributes), param->runLen);
    Record rec;
    Record *currentGroupRecord;
    bool currentGroupActive = false;
    ComparisonEngine ce;
    double sum = 0.0;
    while(localOutPipe.Remove(&rec))
    {
        Record copy;
        copy.Copy(&rec);
        copy.Project(param->groupAttributes->whichAtts, param->groupAttributes->numAtts, ((int*)rec.bits)[1]/sizeof(int) - 1);
        if(!currentGroupActive)
        {
            currentGroupRecord = &copy;
            currentGroupActive = true;
        }
        
        if(ce.Compare(currentGroupRecord, &copy, param->groupAttributes) == 0)
        {
            int ival = 0; double dval = 0;
		param->computeMeFunction->Apply(rec, ival, dval);
		sum += (ival + dval);
	}
        else
        {
            //store old sum and group-by attribtues concatenated in outputPipe
            //and also start new group from here

            // create temperory schema, with one attribute - sum 
            Attribute att = {(char*)"sum", Double};
            Schema sum_schema((char*)"tmp_sum_schema_file", // filename, not used
					   1, // number of attributes
				       &att); // attribute pointer

            // Make a file that contains this sum
            string tempSumFileName = "tmp_sum_data_file" + System::getusec();
            FILE * sum_file = fopen(tempSumFileName.c_str(), "w");
            fprintf(sum_file, "%f|", sum);
            fclose(sum_file);
            sum_file = fopen(tempSumFileName.c_str(), "r");
            // Make record using the above schema and data file
            Record sumRec;
            sumRec.SuckNextRecord(&sum_schema, sum_file);
            fclose(sum_file);

            //glue this record with the one we have from groupAttribute
            int numAttsToKeep = param->groupAttributes->numAtts + 1;
            int attsToKeep[numAttsToKeep];
            attsToKeep[0] = 0;  //for sumRec
            for(int i = 1; i < numAttsToKeep; i++)
            {
                attsToKeep[i] = param->groupAttributes->whichAtts[i-1];
            }
            Record tuple;
            tuple.MergeRecords(&sumRec, &copy, 1, numAttsToKeep - 1, attsToKeep,  numAttsToKeep, 1);
            // Push this record to outPipe
            param->outputPipe->Insert(&tuple);

            //start new group for this record
            currentGroupRecord = &rec;
            currentGroupActive = true;
        }
    }
}
