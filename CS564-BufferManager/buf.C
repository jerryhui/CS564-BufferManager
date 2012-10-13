#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


// 10/8 JH: Pseudo-code added
// 10/10 JH:
// - Implemented function
// - May still need to handle errors from File::writePage()
// - !!Bookkeeping left for those who called allocBuf to do!!
const Status BufMgr::allocBuf(int & frame)
{
    advanceClock();
    int startClock = clockHand; // remembers where we start
    Status rtnStatus=OK;
    
    do {
        // examine page currently at clock hand
        
        // if frame is available, return this frame
        if (!bufTable[clockHand].valid) {
            frame = clockHand;
            bufTable[frame].Clear();
            return OK;
        }
        
        if (bufTable[clockHand].refbit) {
            // clear refbit
            bufTable[clockHand].refbit = false;
        } else {
            if (bufTable[clockHand].pinCnt==0) {
                // no process is referencing this page
                
                if (bufTable[clockHand].dirty) {
                    // write back to disk
                    rtnStatus = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
                    
                    // if I/O not successful return UNIXERR
                    if (rtnStatus==UNIXERR)
                        return UNIXERR;
                    
                    // JH-Note: may need to handle
                    //      BADPAGENO or BADPAGEPTR
                }
                
                frame = clockHand;
                bufTable[frame].Clear();
                return OK;
            }
        }
        
        advanceClock();
    } while (clockHand != startClock);
    
    // all pages are pinned
    return BUFFEREXCEEDED;
}


// 10/8 DM: pseudo code
// 10/10 JH: implemented function
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status rtn=OK;

    if (hashTable->lookup(file, PageNo, frameNo) == OK) {
        // page is already in buffer
        
        bufTable[frameNo].pinCnt++;
        bufTable[frameNo].refbit = true;

        page = &bufPool[frameNo];
    } else {
        // page is not in the buffer
        
        rtn = allocBuf(frameNo);
        if (rtn == OK) {
            if (file->readPage(PageNo, &bufPool[frameNo])==OK) {
                hashTable->insert(file, PageNo, frameNo);
                bufTable[frameNo].Set(file, PageNo);
                
                page = &bufPool[frameNo];
            }
        }
    }

    return rtn;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    // 10/8 DM: pseudo code
    
    // find frame containing (file,PageNo)
    // if not found
    //      return HASHNOTFOUND
    // if pinCnt == 0
    //      return PAGENOTPINNED
    // else
    //      decrement pinCnt of the frame containing(file, PageNo)
    // if dirty == true
    //      set dirty bit
    // return OK

    //Status rtnStatus = OK;

    int frameNo;

    if(HASHNOTFOUND == hashTable->lookup(file, PageNo, frameNo)){
        return HASHNOTFOUND;
    }

    if (bufTable[frameNo].pinCnt <= 0){
        return PAGENOTPINNED;
    }
    else{
        bufTable[frameNo].pinCnt--;
    }

    if(dirty){
        bufTable[frameNo].dirty = true; // JH: better set bool explicitly to true/false instead
    }

    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    
    // 10/8 DM: pseudo code
    // newPageNumber = file->allocatePage()
    // if Unix error, return UNIXERR
    // find an open frame or allocate an open frame
    // if all bufferframes pinned, return BUFFEREXCEEDED
    // insert (file, newPageNumber, openFrameNumber)
    // if hashtable error, return HASHTBLERROR
    // set(file, newPageNumber)    
    // return OK

    if(UNIXERR == file->allocatePage(pageNo)){
        return UNIXERR;
    }
    
    int openFrameNo;
    if(BUFFEREXCEEDED == allocBuf(openFrameNo)){
        return BUFFEREXCEEDED;
    }

    if(HASHTBLERROR == hashTable->insert(file, pageNo, openFrameNo)){
        return HASHTBLERROR;
    }
    
    bufTable[openFrameNo].Set(file, pageNo);
    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


