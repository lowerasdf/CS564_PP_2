/**
 * Group Members:
 * - Bloomest Chandra (9079689528, bjchandra@wisc.edu)
 * - Mei Sun(9081669823, msun252@wisc.edu)
 * - Nan Sun(XXXXXXXXXX, XXXXXXX@wisc.edu)
 * 
 * Purpose:
 * This file consists of the BufMng class that can simulate a buffer manager with clock algorithm
 * for data processing engine, such as database management system. It maintains a buffer pool along
 * with all functionalities to maintain faster access and read and write throughput. Buffer manager
 * itself is responsible for bringing pages from disk to memory as necessary.
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
	/// Case if already in the buffer, otherwise insert to the buffer
	try{
		/// Get a particular frame
		hashTable->lookup(file, pageNo, frame);
		
		/// Set appropriate refbit and pin count
		bufDescTable[frame].refbit = true;
		bufDescTable[frame].pinCnt ++;

		/// Return the page
		page = &bufPool[frame];

	}catch(HashNotFoundException e){
		/// Allocate a new frame
		allocBuf(frame);

		/// Insert newly allocated page to buffer pool, hash table, & description Table
		Page target = file->readPage(pageNo);
		bufPool[frame] = target;
		hashTable->insert(file, pageNo, frame);
		bufDescTable[frame].Set(file, pageNo);

		/// Return the page
		page = &bufPool[frame];
	}
	

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frame;
	/// Unpin page appropriately if found, otherwise do nothing
	try{	
		/// Find the file in hashtable
		hashTable->lookup(file, pageNo, frame);

		///  If pin count has been set to 0 throw exception
		if(bufDescTable[frame].pinCnt == 0){
			throw PageNotPinnedException(file->filename(), pageNo, frame);
		}else{
			/// Decrease number of pin count
			bufDescTable[frame].pinCnt --;
		}
		
		/// Set the appropriate dirty bit
		if(dirty){
			bufDescTable[frame].dirty = true;
		}

	}catch(HashNotFoundException e){
		/// Do nothing
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	/// New page 
	Page newP = file->allocatePage();
	/// Find a frame from buf pool and insert newP into it
	FrameId frame;
	allocBuf(frame);
	bufPool[frame] = newP;

	/// Return page id and page
	page = &bufPool[frame];
	pageNo = bufPool[frame].page_number();
	
	/// Update hash table and description table
	hashTable->insert(file, pageNo, frame);
	bufDescTable[frame].Set(file, pageNo);
}

void BufMgr::flushFile(const File* file) 
{
	/// Scan each frame in the buffer pool
	for (std::uint32_t frameNo = 0; frameNo < numBufs; frameNo++) {

		/// Find the page belong to the given file
		if (bufDescTable[frameNo].file == file) {

			/// If the page found is pinned, throw PagePinnedException
			if (bufDescTable[frameNo].pinCnt != 0) {
				throw PagePinnedException(file->filename(), bufDescTable[frameNo].pageNo, frameNo);
			}

			/// If an invalid page belonging to the file is encountered, throw PagePinnedException
			if (bufDescTable[frameNo].valid == false) {
				throw BadBufferException(frameNo, bufDescTable[frameNo].dirty, bufDescTable[frameNo].valid, bufDescTable[frameNo].refbit);
			}


			/// (a)check the dirty bit
			if (bufDescTable[frameNo].dirty == true) {

				/// Flush the page to the disk
				bufDescTable[frameNo].file->writePage(bufPool[frameNo]);
				/// Set the dirty bit to False
				bufDescTable[frameNo].dirty = false;

			}

			/// (b)delete the page from the hashTable
			hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);

			/// (c)initialize the state of the frame
			bufDescTable[frameNo].Clear();
		}


	}
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frameNo;
	try {
		/// Check if the frame is in the buffer pool
		hashTable->lookup(file, PageNo, frameNo);

		/// If find the frame in the buffer pool, delete the page from the buffer pool
		hashTable->remove(file, PageNo);

		/// Initialize the state of the frame
		bufDescTable[frameNo].Clear();
	}
	catch (HashNotFoundException e) {
		/// If it is not in the buffer pool
	}
	/// Delete the page from file
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
