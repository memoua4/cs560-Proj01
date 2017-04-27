
#include <string.h>
#include <assert.h>
#include "../include/sortMerge.h"

// Error Protocall:


enum ErrCodes {SORT_FAILED, HEAPFILE_FAILED};

static const char* ErrMsgs[] =  {
  "Error: Sort Failed.",
  "Error: HeapFile Failed."
  // maybe more ...
};

struct _rec {
    int	 key;
    char filler[4];
};

static error_string_table ErrTable( JOINS, ErrMsgs );

// sortMerge constructor
sortMerge::sortMerge(
    char*           filename1,      // Name of heapfile for relation R
    int             len_in1,        // # of columns in R.
    AttrType        in1[],          // Array containing field types of R.
    short           t1_str_sizes[], // Array containing size of columns in R
    int             join_col_in1,   // The join column of R 

    char*           filename2,      // Name of heapfile for relation S
    int             len_in2,        // # of columns in S.
    AttrType        in2[],          // Array containing field types of S.
    short           t2_str_sizes[], // Array containing size of columns in S
    int             join_col_in2,   // The join column of S

    char*           filename3,      // Name of heapfile for merged results
    int             amt_of_mem,     // Number of pages available
    TupleOrder      order,          // Sorting order: Ascending or Descending
    Status&         status          // Status of constructor
){
    Status rStatus;
    Status sStatus;
    Status resultStatus;

    char sortedFileOne[] = "sortedFileOne";
    char sortedFileTwo[] = "sortedFileTwo";

    Sort(filename1, sortedFileOne, len_in1, in1, t1_str_sizes, join_col_in1, order, amt_of_mem, rStatus);
    if (rStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, rStatus);
        return;
    }
    Sort(filename2, sortedFileTwo, len_in2, in2, t2_str_sizes, join_col_in2, order, amt_of_mem, sStatus);
    if (sStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, sStatus);
        return;
    }

    HeapFile *heapFileR = new HeapFile(sortedFileOne, rStatus);
    if (rStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, rStatus);
        return;
    }
    HeapFile *heapFileS = new HeapFile(sortedFileTwo, sStatus);
    if (sStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, sStatus);
        return;
    }
    HeapFile *mergeSortResult = new HeapFile(filename3, resultStatus);
    if (resultStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, resultStatus);
        return;
    }

    Scan *scanR = heapFileR->openScan(rStatus);
    if (rStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, rStatus);
        return;
    }
    Scan *scanS = heapFileS->openScan(sStatus);
    if (sStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, sStatus);
        return;
    }

    RID currentRidR;
    RID currentRidS;
    char *currentRecR = new char[sizeof(struct _rec)]; // how big? int?
    char *currentRecS = new char[sizeof(struct _rec)]; // how big? int?
    int currentRLen;
    int currentSLen;
    rStatus = scanR->getNext(currentRidR, currentRecR, currentRLen);
    if (rStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, rStatus);
        return;
    }
    sStatus = scanS->getNext(currentRidS, currentRecS, currentSLen);
    if (sStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, sStatus);
        return;
    }


    // while currentRecR has next && currentRecS has next
    while (rStatus == OK && sStatus == OK)
    {
        //while currentRecR < currentRecS
        if (tupleCmp(currentRecR, currentRecS) < 0)
        {
            rStatus = scanR->getNext(currentRidR, currentRecR, currentRLen);
        }
        // while currentRecR > currentRecS
        else if (tupleCmp(currentRecR, currentRecS) > 0)
        {
            sStatus = scanS->getNext(currentRidS, currentRecS, currentSLen);
        }
        else {
            // Now we know currentRecR == currentRecS, so we output the match
            // Start by outputting the current match
            char *joinedTuple = new char[sizeof(struct _rec) * 2];
            memcpy(joinedTuple, currentRecR, currentRLen);
            memcpy(joinedTuple + sizeof(struct _rec), currentRecS, currentSLen);
            RID ignored;
            mergeSortResult->insertRecord(joinedTuple, sizeof(struct _rec) * 2, ignored);

            //
            RID oldScanS;
            oldScanS.pageNo = currentRidS.pageNo;
            oldScanS.slotNo = currentRidS.slotNo;

            // Now output all the matching tuples after from S with the current tuple from R
            sStatus = scanS->getNext(currentRidS, currentRecS, currentSLen);
            while (sStatus == OK && tupleCmp(currentRecR, currentRecS) == 0)
            {
                memcpy(joinedTuple, currentRecR, currentRLen);
                memcpy(joinedTuple + sizeof(struct _rec), currentRecS, currentSLen);
                sStatus = mergeSortResult->insertRecord(joinedTuple, sizeof(struct _rec) * 2, ignored);
                if (sStatus != OK) {
                    cout << "Is this possible?" << sStatus << endl;
                    //minibase_errors.show_errors();
                }
                sStatus = scanS->getNext(currentRidS, currentRecS, currentSLen);
            }

            //
            RID oldScanR;
            oldScanR.pageNo = currentRidR.pageNo;
            oldScanR.slotNo = currentRidR.slotNo;


            // Now output all the matching tuples from R with the current tuple from S
            rStatus = scanR->getNext(currentRidR, currentRecR, currentRLen);
            sStatus = heapFileS->getRecord(oldScanS, currentRecS, currentSLen);
            // cerr << "Output 2 = " << currentRecS << " and " << oldRecS << endl;;
            while (rStatus == OK && tupleCmp(currentRecR, currentRecS) == 0)
            {
                memcpy(joinedTuple, currentRecR, currentRLen);
                memcpy(joinedTuple + sizeof(struct _rec), currentRecS, currentSLen);
                rStatus = mergeSortResult->insertRecord(joinedTuple, sizeof(struct _rec) * 2, ignored);
                if (rStatus != OK)
                    cout << "Is this possible?" << rStatus << endl;
                rStatus = scanR->getNext(currentRidR, currentRecR, currentRLen);
            }

            delete joinedTuple;

            //cout << oldScanS.pageNo << ", " << oldScanS.slotNo << endl;

            scanS->position(oldScanS);
            scanR->position(oldScanR);

            //cout << currentRidS.pageNo << ", " << currentRidS.slotNo << endl;
            sStatus = scanS->getNext(currentRidS, currentRecS, currentSLen);
            //cout << currentRidS.pageNo << ", " << currentRidS.slotNo << endl;
            rStatus = scanR->getNext(currentRidR, currentRecR, currentRLen);
        }
    }

    delete currentRecR;
    delete currentRecS;

    delete scanR;
    delete scanS;

    heapFileR->deleteFile();
    heapFileS->deleteFile();
    delete mergeSortResult;

    status = OK;
}

// sortMerge destructor
sortMerge::~sortMerge()
{
}
