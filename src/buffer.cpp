/**
 * TODO add name + student ID + purpose of file (see page 9)
 * Bloomest Jansen Chandra (9079689528, bjchandra@wisc.edu)
 * Mei Sun(9081669823, msun252@wisc.edu)
 * 
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"


namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

void BufMgr::advanceClock()
{
	/// The modulo operation here is to make sure that the flow is circular 
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	/// Counter that counts the the number of frames with non-empty pin count
	unsigned int counter = 0;
	
	while(true) {
		/// If all frames have at least 1 pin count, throw the exception
		if(counter == numBufs) {
			throw BufferExceededException();
		}

		/// Only consider a page if it's valid, otherwise it can be allocated
		if(bufDescTable[clockHand].valid) {
			/// Change refbit to 0 if it is 1 and move on, otherwise check pin count
			if(bufDescTable[clockHand].refbit) {
				bufDescTable[clockHand].refbit = false;
				advanceClock();
			} else {
				/// Only consider frame with pin count 0, otherwise move on
				if(bufDescTable[clockHand].pinCnt > 0) {
					counter += 1;
					advanceClock();
				} else {
					/// Flush the appropriate page if it's been modified
					if(bufDescTable[clockHand].dirty) {
						bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
					}

					/// remove the appropriate entry from the hash table
					hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
					
					/// Clear current frame and return the output via the given pointer
					bufDescTable[clockHand].Clear();
					frame = clockHand;
					break;
				}
			}
		} else {
			frame = clockHand;
			break;
		}
	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frame;
	try{
		hashTable->lookup(file, pageNo, frame);
		//if already in the buffer
		bufDescTable[frame].pinCnt ++;
		page = &bufPool[frame];

	}catch(HashNotFoundException e){
		//if not in the buffer, insert to the buffer
		allocBuf(frame);
		Page target = file->readPage(pageNo);
		bufPool[frame] = target;
		hashTable->insert(file, pageNo, frame);
		bufDescTable[frame].Set(file, pageNo);
		page = &target;
	}
	

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frame;
	try{	
		//find the file in hashtable
		hashTable->lookup(file, pageNo, frame);

		//if pin count has been set to 0 throw exception
		if(bufDescTable[frame].pinCnt == 0){
			throw PageNotPinnedException(file->filename(), pageNo, frame);
		}else{
			//decrease number of pin count
			bufDescTable[frame].pinCnt --;
		}
		
		if(dirty){
			bufDescTable[frame].dirty = true;
		}

	}catch(HashNotFoundException e){
		//do nothing
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	//new page 
	Page newP = file->allocatePage();
	//find a frame from buf pool and insert newP into it
	FrameId frame;
	allocBuf(frame);
	hashTable->insert(file, pageNo, frame);
	bufDescTable[frame].Set(file, pageNo);
	bufPool[frame] = newP;

	//return page id and page
	pageNo = newP.page_number();
	page = &newP;
	
}

/**
* flush the dirty pages of a particular file in the buffer pool to the disk 
*
* @param file  the file the dirty page to be flushed belong to 
* @throws PagePinnedException if the page of the file found in the buffer pool is pinned
* @throws BadBufferException  if an invalid page belonging to the file is encountered
*/
void BufMgr::flushFile(const File* file) 
{
	//Scan each frame in the buffer pool
	for (std::uint32_t frameNo = 0; frameNo < numBufs; frameNo++) {

		//find the page belong to the given file
		if (bufDescTable[frameNo].file == file) {

			//if the page found is pinned, throw PagePinnedException
			if (bufDescTable[frameNo].pinCnt != 0) {
				throw PagePinnedException(file->filename(), bufDescTable[frameNo].pageNo, frameNo);
			}

			// if an invalid page belonging to the file is encountered, throw PagePinnedException
			if (bufDescTable[frameNo].valid == false) {
				throw BadBufferException(frameNo, bufDescTable[frameNo].dirty, bufDescTable[frameNo].valid, bufDescTable[frameNo].refbit);
			}


			//(a)check the dirty bit
			if (bufDescTable[frameNo].dirty == true) {

				//flush the page to the disk
				bufDescTable[frameNo].file->writePage(bufPool[frameNo]);
				//set the dirty bit to False
				bufDescTable[frameNo].dirty = false;

			}

			//(b)delete the page from the hashTable
			hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);

			//(c)initialize the state of the frame
			bufDescTable[frameNo].Clear();
		}


	}
}

/**
* delete a particular page from a file, also remove it from the buffer pool if it is allocated there
* 
* @param file  the file the page to be removed from 
* @param PageNo the number of the page to be disposed
*/
void BufMgr::disposePage(File* file, const PageId PageNo)
{
	//check if the frame is in the buffer pool
	for (std::uint32_t frameNo = 0; frameNo < numBufs; frameNo++) {
		if (bufDescTable[frameNo].pageNo == PageNo) {

			//if find the frame in the buffer pool, delete the page from the buffer pool
			hashTable->remove(file, PageNo);

			//initialize the state of the frame
			bufDescTable[frameNo].Clear();
		}
	}

	//delete the page from file
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
