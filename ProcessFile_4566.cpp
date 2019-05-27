#include"ProcessFile.h"
extern char sourceFileName[FILENAME_LENGTH];
extern char targetFileName[FILENAME_LENGTH];
extern char outputFileName[FILENAME_LENGTH];
extern char outputSmallFileName[FILENAME_LENGTH];
extern int smallFileRecs;

void ProcessFile::findDifferences()
{
	DiffFilePtr =  fopen(diffFileName.c_str(), "w");
	FILE* file = fopen(sourceFileName, "r"); /* should check the result */
    char line[MAX_RECORD_LEN]={0};	

	long seekPos = mFseekStart;
	
	if (mThreadNbr > 0)
	{
		fseek( file, mFseekStart, SEEK_SET );
		fgets(line, sizeof(line), file); // skip first line as it can be incomplete
		
		seekPos += strlen(line);
	}
	else
	{
		fseek( file, mFseekStart, SEEK_SET );
	}
	
    while (fgets(line, sizeof(line), file) && (seekPos < mFseekEnd)) 
	{
		//cout << "Thread Nbr: " << mThreadNbr << ", Processing line: " << line << "\n";
		
		rowsProcessed++;
		char key[MAX_KEY_LEN] ={0}; 
		char hasVal[hasValSize]={0};
		int fno=1;
		bool exitFlag = false;
		int j=0;
		int lineSize = strlen(line);
		
		
		for (int i=0; i<lineSize; i++)
		{
			if (line[i] == '|')
			{
				fno++;
				continue;
			}
			switch (fno)
			{
				case 1: key[i]=line[i];
				break;
				case 2: hasVal[j]=line[i];
				j++;
				break;
				
				default: exitFlag=true;
				//mTargetFileMap.insert(make_pair(atoi(key), value));
				// now search in the hashes
				bool found = false;
				
				for (int i=0; i<targetHashes.size(); i++)
				{
					std::unordered_map<string,Value>::iterator itr = targetHashes[i].find(string(key));
					if (itr != targetHashes[i].end())
					{
						if (strcmp( hasVal, itr->second.hasVal) != 0)
						{
							char tgtLine[MAX_RECORD_LEN] = {0};
							GetTgtLine(itr->second.fseekLoc, tgtLine);
							
							string str = FormatDelta(line, tgtLine, key);
							
							fprintf(DiffFilePtr, "%s\n",str.c_str());
							
							rowsNotMatched++;
							
						}
						found = true;
						break;
					}
				}
				
				if (!found)
				{
					char tmp[1] = {0};
					
					string str = FormatDelta(line, tmp, key);
					fprintf(DiffFilePtr, "%s\n",str.c_str());
					rowsNotMatched++;
				}
				
				break;
			}
			
			if (exitFlag)
			{
				break;
			}
		}
		
		seekPos += lineSize;
    }
    /* may check feof here to make a difference between eof and io failure -- network
       timeout for instance */

    fclose(file);
	fclose(DiffFilePtr);
}

string ProcessFile::FormatDelta(char  *line, char *tgtLine, char  *key)
{
	long line_len = strlen(line);
	if (line_len > 0 && line[line_len-1] == '\n') 
	line[--line_len] = '\0';
	
	long tgtLine_len = strlen(tgtLine);
	if (tgtLine_len > 0 && tgtLine[tgtLine_len-1] == '\n') 
	tgtLine[--tgtLine_len] = '\0';
	
	// remove first two fields
	char line_actual[MAX_RECORD_LEN] = {0};
	char tgt_line_actual[MAX_RECORD_LEN] = {0};
	
	int idx = 0;
	int pipeCnt = 0;
	
	while (idx < (line_len - 1))
	{
		if ( line[idx] == '|' )
		{
			pipeCnt++;
			
			if (pipeCnt == 2)
			{
				strcpy(line_actual, line + idx + 1);
				break;
			}
		}
		idx++;
	}
	idx = 0;
	pipeCnt=0;
	while (idx < (tgtLine_len - 1))
	{
		if ( tgtLine[idx] == '|' )
		{
			pipeCnt++;
			
			if (pipeCnt == 2)
			{
				strcpy(tgt_line_actual, tgtLine + idx + 1);
				break;
			}
		}
		idx++;
	}
	string record = string (key) + string("~") + string(line_actual) + string("~") + string(tgt_line_actual);
	
	return record;
}
void ProcessFile::GetTgtLine(long fseekLoc, char *line)
{
	FILE* file = fopen(targetFileName, "r"); /* should check the result */
	
	fseek( file, fseekLoc, SEEK_SET );
	fgets(line, MAX_RECORD_LEN, file);
	fclose(file);
}
void ProcessFile::populateTargetMap()
{
    FILE* file = fopen(targetFileName, "r"); /* should check the result */
    char line[MAX_RECORD_LEN]={0};	
	long seekPos = mTgtFseekStart;
	
	if (mThreadNbr > 0)
	{
		fseek( file, mTgtFseekStart , SEEK_SET );
		fgets(line, sizeof(line), file); // skip first line as it can be incomplete
		
		seekPos += strlen(line);
	}
	else
	{
		fseek( file, mTgtFseekStart, SEEK_SET );
	}
	int lnCnt = 0;
    while (fgets(line, sizeof(line), file) && (seekPos < mTgtFseekEnd)) 
	{
		lnCnt++;
		//cout << "lnCnt : " << lnCnt;
		//cout << "Thread Nbr: " << mThreadNbr << ", Processing line: " << line << "\n";

		char key[MAX_KEY_LEN] ={0}; 
		char hasVal[hasValSize]={0};
		int fno=1;
		bool exitFlag = false;
		int j=0;
		int lineSize = strlen(line);
		
		
		for (int i=0; i<lineSize; i++)
		{
			if (line[i] == '|')
			{
				fno++;
				continue;
			}
			switch (fno)
			{
				case 1: key[i]=line[i];
				break;
				case 2: hasVal[j]=line[i];
				j++;
				break;
				
				default: exitFlag=true;
				Value value;
				strcpy(value.hasVal, hasVal);
				value.fseekLoc = seekPos;
				mTargetFileMap.insert(make_pair(string(key), value));
				break;
			}
			
			if (exitFlag)
			{
				break;
			}
		}
		
		seekPos += lineSize;
		//		cout << "seekPos: " << seekPos << "\n";
		//cout << "mFseekEnd: " << mFseekEnd << "\n";
    }
    /* may check feof here to make a difference between eof and io failure -- network
       timeout for instance */

    fclose(file);
}
