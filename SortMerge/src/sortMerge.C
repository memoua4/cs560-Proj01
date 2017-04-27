
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

// We need this so we save enough space to hold the records we're given in the test driver
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
    // Declare status variables for each of the files we will use (R heapfile, S heapfile, and Output heapfile)
    Status rStatus;
    Status sStatus;
    Status resultStatus;

    // We create two files that we sort to, here we declare the file names
    char sortedFileOne[] = "sortedFileOne";
    char sortedFileTwo[] = "sortedFileTwo";

    // Call sort with the correct parameters on relation R, ensure that the status is OK
	Sort(filename1, sortedFileOne, len_in1, in1, t1_str_sizes, join_col_in1, order, amt_of_mem, rStatus);
    if (rStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, rStatus);
        return;
    }
    // Call sort with the correct parameters on relation S, ensure that the status is OK
    Sort(filename2, sortedFileTwo, len_in2, in2, t2_str_sizes, join_col_in2, order, amt_of_mem, sStatus);
    if (sStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, sStatus);
        return;
    }

    // Create a new heapfile for the now sorted relation R
    HeapFile *heapFileR = new HeapFile(sortedFileOne, rStatus);
    if (rStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, rStatus);
        return;
    }
    // Create a new heapfile for the now sorted relation S
    HeapFile *heapFileS = new HeapFile(sortedFileTwo, sStatus);
    if (sStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, sStatus);
        return;
    }
    // Create a new heapfile to write to, it will contain the records that have been merged together.
    HeapFile *mergeSortResult = new HeapFile(filename3, resultStatus);
    if (resultStatus != OK) {
        status = MINIBASE_CHAIN_ERROR(JOINS, resultStatus);
        return;
    }

    // Initiate scans on S and R. We use these to traverse the two sorted heapfiles together.
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

    // The current RID in R & S that we are currently at in our scans
    RID currentRidR;
    RID currentRidS;
    // The pointers to the current record in R & S given to us by the scans
    char *currentRecR = new char[sizeof(struct _rec)];
    char *currentRecS = new char[sizeof(struct _rec)];
    // The current length of the record in R & S, unused at the moment
    int currentRLen;
    int currentSLen;
    // Grab the first tuple from the R & S scan. Then we begin our merge process
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


    // While both scans still have entries, their statuses will remain OK
    while (rStatus == OK && sStatus == OK)
    {
        // Check if the current tuple in R is less than the one in S. If this is the case, we advance
        // the lower one (R), and move on to the next iteration
        if (tupleCmp(currentRecR, currentRecS) < 0)
        {
            rStatus = scanR->getNext(currentRidR, currentRecR, currentRLen);
        }
        // Check if the current tuple in R is greater than the one in S. If this is the case, we advance
        // the lower one (S), and move on to the next iteration
        else if (tupleCmp(currentRecR, currentRecS) > 0)
        {
            sStatus = scanS->getNext(currentRidS, currentRecS, currentSLen);
        }
        else {
            // Now we know currentRecR == currentRecS, so we output the match
            // Start by outputting the current match, which means we need to allocate space to hold
            // both the tuple from R, and the tuple from S
            char *joinedTuple = new char[sizeof(struct _rec) * 2];
            // Perform a memcpy to copy the S & R record into the new tuple
            memcpy(joinedTuple, currentRecR, currentRLen);
            memcpy(joinedTuple + sizeof(struct _rec), currentRecS, currentSLen);
            RID ignored;
            // Insert the record into the merge sort heapfile. We ignore the RID it returns
            mergeSortResult->insertRecord(joinedTuple, sizeof(struct _rec) * 2, ignored);

            // Now output all the matching tuples after from S with the current tuple from R
            // We save off the old S scan RID to return to after we perform the merge
            RID oldScanS;
            oldScanS.pageNo = currentRidS.pageNo;
            oldScanS.slotNo = currentRidS.slotNo;
            // Save off the currentSRec
            char *tempSRec = new char[ sizeof(struct _rec)];
            memcpy(tempSRec, currentRecS, currentSLen);

            // Grab the next tuple from the S scan
            sStatus = scanS->getNext(currentRidS, currentRecS, currentSLen);
            // Loop while the status is OK, and the current record from S matches the current tuple from R
            while (sStatus == OK && tupleCmp(currentRecR, currentRecS) == 0)
            {
                // We got another match, so output that tuple to the merge sort heapfile
                memcpy(joinedTuple, currentRecR, currentRLen);
                memcpy(joinedTuple + sizeof(struct _rec), currentRecS, currentSLen);
                mergeSortResult->insertRecord(joinedTuple, sizeof(struct _rec) * 2, ignored);
                // Advance to the next record in S to compare to
                sStatus = scanS->getNext(currentRidS, currentRecS, currentSLen);
            }

            // Now output all the matching tuples after from R with the current tuple from S
            // We save off the old R scan RID to return to after we perform the merge
            RID oldScanR;
            oldScanR.pageNo = currentRidR.pageNo;
            oldScanR.slotNo = currentRidR.slotNo;
            // We'll need to ensure that we restore currentRecS
            delete currentRecS;
            currentRecS = tempSRec;

            // Grab the next tuple from the S scan
            rStatus = scanR->getNext(currentRidR, currentRecR, currentRLen);
            // Loop while the status is OK, and the current record from R matches the current tuple from S
            while (rStatus == OK && tupleCmp(currentRecR, currentRecS) == 0)
            {
                // We got another match, so output that tuple to the merge sort heapfile
                memcpy(joinedTuple, currentRecR, currentRLen);
                memcpy(joinedTuple + sizeof(struct _rec), currentRecS, currentSLen);
                rStatus = mergeSortResult->insertRecord(joinedTuple, sizeof(struct _rec) * 2, ignored);
                // Advance to the next record in R to compare to
                rStatus = scanR->getNext(currentRidR, currentRecR, currentRLen);
            }

            // Free the memory occupied by the joined tuple
            delete joinedTuple;

            // Reposition the scans to where they started at before the advancements in the above code
            scanS->position(oldScanS);
            scanR->position(oldScanR);

            // Advance to the next record
            sStatus = scanS->getNext(currentRidS, currentRecS, currentSLen);
            rStatus = scanR->getNext(currentRidR, currentRecR, currentRLen);
        }
    }
    
    // Free any memory used, and return an OK status

    delete currentRecR;
    delete currentRecS;

    delete scanR;
    delete scanS;

    heapFileR->deleteFile();
    heapFileS->deleteFile();

    delete heapFileR;
    delete heapFileS;
    delete mergeSortResult;

    status = OK;
}

// sortMerge destructor does nothing
sortMerge::~sortMerge()
{
}
