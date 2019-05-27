#ifndef PROCESSFILE_H_
#define PROCESSFILE_H_

#define hasValSize 100
#define FILENAME_LENGTH 1000
#define MAX_RECORD_LEN 5000
#define MAX_KEY_LEN 500

#include<vector>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <string>
#include <unordered_map>
#include <thread>
#include <map>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h> 

using namespace std;

extern char outputPath[FILENAME_LENGTH];

struct Value
{
	char hasVal[hasValSize];
	long fseekLoc;
};
class ProcessFile
{
	long mFseekStart;
	long mFseekEnd;
	long mTgtFseekStart;
	long mTgtFseekEnd;
	int mThreadNbr;
	
	std::unordered_map<string,Value> mSourceFileMap;
	std::unordered_map<string,Value> mTargetFileMap;
	vector<string> diffRows;
	FILE *DiffFilePtr;
	
	
	public:
	
	long rowsProcessed;
	long rowsNotMatched;
	string diffFileName;
	
	ProcessFile(int _threadNbr, long _seekStart, long _seekEnd, long _TgtSeekStart, long _TgtSeekEnd)
	{
		mThreadNbr = _threadNbr;
		if (_seekStart > 0)
		{
			mFseekStart = _seekStart - 1; // this to ensure that first will always be skipped by a thread and that line will be processed by previous thread.
		}
		else
		{
			mFseekStart = _seekStart;
		}
		mFseekEnd = _seekEnd;
		if (_TgtSeekStart > 0)
		{
			mTgtFseekStart = _TgtSeekStart - 1; // this to ensure that first will always be skipped by a thread and that line will be processed by previous thread.
		}
		else
		{
			mTgtFseekStart = _TgtSeekStart;
		}
		mTgtFseekEnd = _TgtSeekEnd;
		rowsProcessed = 0;
		rowsNotMatched = 0;
		diffFileName = string(outputPath) + "diff_" + to_string(mThreadNbr);
	}
	
	void populateTargetMap();
	
	void findDifferences();
	
	void processFirstAndLines();
	
	unordered_map<string,Value> GetTargetFileMap();
	
	void FreeTargetMap()
	{
		mTargetFileMap.clear();
	}
	
	vector<string> &GetDiffRows()
	{
		return diffRows;
	}
	
	void showSize()
	{
		cout << "Size: \n";
		cout << "mTargetFileMap : " << mTargetFileMap.size() << "\n"; 
	}
	
	void GetTgtLine(long fseekLoc, char *);
	
	string FormatDelta(char  *line, char *tgtLine, char  *key);
};

#endif